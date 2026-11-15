use std::{cell::RefCell, collections::BTreeSet, iter::Peekable, rc::Rc, slice::Iter};

use anyhow::{Context, Result, bail, ensure};

use super::{PATH, Step, Traversers, condition, parse_chain, qualified, scalar_identity, text};
use super::{
    repeat::{OutputLimit, Repeat},
    variants::{Kind, extend_tags},
};
use crate::catalog::ident;

#[derive(Clone)]
pub(super) struct Group {
    pub(super) key: String,
    pub(super) input: String,
}

struct EmissionTail<'a> {
    limit: OutputLimit,
    productive: Vec<Step<'a>>,
    dedup_at: Option<usize>,
}

fn productive_step(step: &Step<'_>) -> bool {
    matches!(
        (step.name, step.args.as_slice()),
        ("identity" | "path" | "simplePath" | "id" | "label", [])
            | ("values", [_])
            | (
                "valueMap" | "has" | "hasKey" | "hasId" | "hasLabel" | "is" | "not",
                _
            )
    )
}

fn barrier(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "mean"
            | "sum"
            | "min"
            | "max"
            | "groupCount"
            | "limit"
            | "range"
            | "skip"
            | "dedup"
            | "order"
    )
}

fn has_barrier(steps: &[Step<'_>]) -> bool {
    steps.iter().any(|step| barrier(step.name))
}

enum LoopEnd<'a, 'q> {
    Until(&'a [Step<'q>]),
    Times(u64),
    Exhausted,
}

struct Emit<'a, 'q> {
    predicate: &'a [Step<'q>],
    before: bool,
}

impl Traversers<'_> {
    pub(super) fn fresh(&mut self, purpose: &str) -> String {
        let column = format!("__graphlake_gremlin_{purpose}_{}", self.next_column);
        self.next_column += 1;
        column
    }

    pub(super) fn select_columns(&self, alias: &str) -> String {
        let mut columns = self.visible(alias);
        if self.path {
            columns.push(qualified(alias, PATH));
        }
        columns.extend(
            self.tags
                .keys()
                .map(|tag| qualified(alias, &super::variants::tag_column(tag))),
        );
        columns.extend(self.order.iter().map(|(name, _)| qualified(alias, name)));
        columns.extend(
            self.control_columns
                .iter()
                .map(|name| qualified(alias, name)),
        );
        columns.join(", ")
    }

    pub(super) fn sequence_order(&self) -> String {
        let columns = self.select_columns("v");
        if self.order.is_empty() {
            columns
        } else {
            format!("{}, {columns}", self.order_terms())
        }
    }

    pub(super) fn count(&mut self) {
        self.reduce(
            "count(*)",
            "coalesce(q.aggregate, CAST(0 AS BIGINT))",
            Kind::Count,
        );
    }

    pub(super) fn mean(&mut self) -> Result<()> {
        ensure!(
            self.kinds()
                .iter()
                .all(|kind| !matches!(kind, Kind::Element(_))),
            "mean() requires numeric values"
        );
        let value = if self.variants.is_empty() {
            "v.value".into()
        } else {
            let arms = (0..self.variants.len())
                .map(|index| {
                    format!(
                        "WHEN {index} THEN __graphlake_gremlin_number({})",
                        qualified("v", &super::variants::slot(index))
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            format!("CASE v.{} {arms} END", super::variants::KIND)
        };
        let value = if self.variants.is_empty() {
            format!("__graphlake_gremlin_number({value})")
        } else {
            value
        };
        self.reduce(&format!("avg({value})"), "q.aggregate", Kind::Double);
        self.restrict("v.value IS NOT NULL");
        Ok(())
    }

    pub(super) fn sum(&mut self) -> Result<()> {
        let kinds = self.kinds();
        ensure!(
            kinds.iter().all(|kind| !matches!(kind, Kind::Element(_))),
            "sum() requires numeric values"
        );
        let inputs = if self.variants.is_empty() {
            vec!["v.value".into()]
        } else {
            (0..kinds.len())
                .map(|index| qualified("v", &super::variants::slot(index)))
                .collect()
        };
        let sources = kinds
            .into_iter()
            .flat_map(|kind| match kind {
                Kind::Sum(sources) => sources,
                other => vec![other],
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let value = format!("__graphlake_numeric_input({})", inputs.join(", "));
        self.reduce(
            &format!("sum({value})"),
            "__graphlake_gremlin_sum_zero(q.aggregate)",
            Kind::Sum(sources),
        );
        Ok(())
    }

    pub(super) fn extremum(&mut self, maximum: bool) -> Result<()> {
        let kinds = self.kinds();
        ensure!(
            kinds.iter().all(|kind| !matches!(kind, Kind::Element(_))),
            "min() and max() require values"
        );
        let value = self.variant_value("v");
        let key = format!("__graphlake_gremlin_order({value})");
        let result = if let [kind] = kinds.as_slice() {
            kind.clone()
        } else {
            let sources = kinds
                .into_iter()
                .flat_map(|kind| match kind {
                    Kind::Extremum(sources) => sources,
                    other => vec![other],
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            Kind::Extremum(sources)
        };
        self.reduce(
            &crate::cypher::extremum(&value, &key, maximum),
            "q.aggregate",
            result,
        );
        self.restrict("v.value IS NOT NULL");
        Ok(())
    }

    pub(super) fn reduce(&mut self, aggregate: &str, value: &str, kind: Kind) {
        let path = if self.path {
            format!(", make_array({}) AS {PATH}", scalar_identity("v.value"))
        } else {
            String::new()
        };
        let source = if let Some(group) = &self.group {
            let key = ident(&group.key);
            let controls = self
                .control_columns
                .iter()
                .map(|column| qualified("g", column))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {value} AS value, {controls}
                 FROM (SELECT DISTINCT {controls} FROM ({seed}) AS g) AS g
                 LEFT JOIN (SELECT {key}, {aggregate} AS aggregate FROM ({input}) AS v GROUP BY {key}) AS q
                 ON g.{key} = q.{key}",
                seed = group.input, input = self.sql
            )
        } else {
            format!(
                "SELECT {value} AS value FROM (SELECT {aggregate} AS aggregate FROM ({}) AS v) AS q",
                self.sql
            )
        };
        self.sql = format!("SELECT v.*{path} FROM ({source}) AS v");
        self.entity = None;
        self.variants.clear();
        self.scalar_kind = kind;
        self.tags.clear();
        self.order.clear();
    }

    pub(super) fn range(&mut self, low: u64, high: Option<u64>) {
        if let Some(group) = &self.group {
            let partition = qualified("v", &group.key);
            self.retain_ranked(&partition, low, high);
        } else {
            let limit = high.map_or_else(String::new, |high| format!(" LIMIT {}", high - low));
            let mut order = self.order_clause();
            if !self.variants.is_empty() && high != Some(low) {
                // Later steps can read this page once per label. They need the
                // same rows even when the input order is unspecified or tied.
                order.push_str(if order.is_empty() { " ORDER BY " } else { ", " });
                order.push_str(&self.variant_order("v"));
            }
            self.sql = format!(
                "SELECT v.* FROM ({}) AS v{order}{limit} OFFSET {low}",
                self.sql
            );
        }
    }

    pub(super) fn dedup(&mut self, key: &str) {
        if let Some(group) = &self.group {
            let partition = format!("{}, {key}", qualified("v", &group.key));
            self.retain_ranked(&partition, 0, Some(1));
        } else {
            let order = if self.order.is_empty() {
                String::new()
            } else {
                format!(" ORDER BY {key}, {}", self.order_terms())
            };
            self.sql = format!(
                "SELECT DISTINCT ON ({key}) v.* FROM ({}) AS v{order}",
                self.sql
            );
        }
    }

    fn retain_ranked(&mut self, partition: &str, low: u64, high: Option<u64>) {
        let order = self.sequence_order();
        let rank = format!(
            "row_number() OVER (PARTITION BY {partition} ORDER BY {order} ROWS UNBOUNDED PRECEDING)"
        );
        let predicate = match (low, high) {
            (0, Some(high)) => format!("{rank} <= {high}"),
            (low, Some(high)) => format!("{rank} > {low} AND {rank} <= {high}"),
            (low, None) => format!("{rank} > {low}"),
        };
        self.sql = format!("SELECT v.* FROM ({}) AS v QUALIFY {predicate}", self.sql);
    }

    fn begin_iteration(&mut self) {
        if self.group.is_some() {
            return;
        }
        let key = self.fresh("iteration_group");
        self.sql = format!(
            "SELECT v.*, CAST(0 AS BIGINT) AS {} FROM ({}) AS v",
            ident(&key),
            self.sql
        );
        self.control_columns.push(key.clone());
        self.group = Some(Group {
            key: key.clone(),
            input: String::new(),
        });
    }

    pub(super) fn branch(&mut self, mode: &str, args: &[&str], depth: usize) -> Result<()> {
        ensure!(depth < 16, "Gremlin nesting exceeds 16");
        let (predicate, branches) = if mode == "choose" {
            let [predicate, branches @ ..] = args else {
                bail!("choose() requires a predicate and branches")
            };
            ensure!(
                (1..=2).contains(&branches.len()),
                "choose() requires a true branch and an optional false branch"
            );
            (Some(*predicate), branches)
        } else {
            ensure!(!args.is_empty(), "{mode}() requires branches");
            (None, args)
        };
        let seed = self.fresh("seed");
        let row = self.fresh("row");
        let priority = self.fresh("branch");
        let rank = self.fresh("branch_order");
        let matches = self.fresh("matches");
        let columns = self.select_columns("v");
        let seed_sql = format!(
            "SELECT {columns}, row_number() OVER (ORDER BY {} ROWS UNBOUNDED PRECEDING) AS {} FROM ({}) AS v",
            self.sequence_order(),
            ident(&row),
            self.sql
        );
        let mut clauses = vec![format!("{} AS ({seed_sql})", ident(&seed))];
        if let Some(predicate) = predicate {
            let mut check = self.clone();
            check.sql = format!("SELECT * FROM {}", ident(&seed));
            check.enter_branch(&row);
            check.ctes.extend(clauses.iter().cloned());
            let mut steps = parse_chain(predicate, "__")?;
            steps.push(Step {
                name: "limit",
                args: vec!["1"],
            });
            check.apply(&steps, depth + 1)?;
            self.labels.extend(check.labels);
            self.next_column = self.next_column.max(check.next_column);
            clauses.push(format!(
                "{} AS (SELECT DISTINCT {} FROM ({}) AS matched)",
                ident(&matches),
                ident(&row),
                check.sql
            ));
        }
        let mut output = Vec::new();
        let mut pending = ident(&seed);
        for index in 0..if predicate.is_some() {
            2
        } else {
            branches.len()
        } {
            let mut branch = self.clone();
            branch.ctes.extend(clauses.iter().cloned());
            branch.sql = format!("SELECT s.* FROM {pending} AS s");
            if predicate.is_some() {
                let join = if index == 0 {
                    "LEFT SEMI JOIN"
                } else {
                    "LEFT ANTI JOIN"
                };
                branch.sql.push_str(&format!(
                    " {join} {} AS m ON {} = {}",
                    ident(&matches),
                    qualified("s", &row),
                    qualified("m", &row)
                ));
            }
            branch.enter_branch(&row);
            if let Some(text) = branches.get(index) {
                let mut steps = parse_chain(text, "__")?;
                if mode == "map" {
                    steps.push(Step {
                        name: "limit",
                        args: vec!["1"],
                    });
                }
                branch.apply(&steps, depth + 1)?;
            }
            self.next_column = self.next_column.max(branch.next_column);
            if mode == "coalesce" && index + 1 < branches.len() {
                let produced = ident(&self.fresh("coalesce_produced"));
                let remaining = ident(&self.fresh("coalesce_remaining"));
                let row = ident(&row);
                clauses.push(format!("{produced} AS ({})", branch.sql));
                // Subsequent branches only see traversers for which this
                // branch was unproductive, including inside nested repeats.
                clauses.push(format!(
                    "{remaining} AS (SELECT s.* FROM {pending} AS s LEFT ANTI JOIN
                     (SELECT DISTINCT {row} FROM {produced}) AS m ON s.{row} = m.{row})"
                ));
                branch.sql = format!("SELECT * FROM {produced}");
                pending = remaining;
            }
            output.push(branch);
        }
        let ordered = output.iter().any(|branch| !branch.order.is_empty());
        for (index, branch) in output.iter_mut().enumerate() {
            if ordered {
                let order = branch.sequence_order();
                branch.sql = format!(
                    "SELECT v.*, row_number() OVER (PARTITION BY {} ORDER BY {order} ROWS UNBOUNDED PRECEDING) AS {} FROM ({}) AS v",
                    ident(&row),
                    ident(&rank),
                    branch.sql
                );
                branch.control_columns.push(rank.clone());
            }
            branch.order.clear();
            if ordered {
                branch.sql = format!(
                    "SELECT v.*, {index} AS {} FROM ({}) AS v",
                    ident(&priority),
                    branch.sql
                );
                branch.control_columns.push(priority.clone());
            }
        }
        let result = self.combine(&output);
        if ordered {
            self.order.extend([
                (rank, false),
                (priority.clone(), false),
                (row.clone(), false),
            ]);
        }
        let projection = self.select_columns("v");
        let result = format!("SELECT {projection} FROM ({result}) AS v");
        self.sql = format!("WITH {} {result}", clauses.join(", "));
        Ok(())
    }

    pub(super) fn enter_branch(&mut self, row: &str) {
        self.control_columns
            .extend(self.order.drain(..).map(|(column, _)| column));
        self.control_columns.push(row.into());
        self.group = Some(Group {
            key: row.into(),
            input: self.sql.clone(),
        });
    }

    pub(super) fn repeat_step<'q>(
        &mut self,
        first: &Step<'q>,
        steps: &mut Peekable<Iter<'_, Step<'q>>>,
        depth: usize,
    ) -> Result<()> {
        ensure!(depth < 16, "Gremlin nesting exceeds 16");
        let mut end = None;
        let mut emit = None;
        let mut modulate = |step: &Step<'q>, before: bool| -> Result<()> {
            match (step.name, step.args.as_slice()) {
                ("emit", [] | [_]) => {
                    ensure!(emit.is_none(), "repeat() accepts one emit()");
                    emit = Some((step.args.first().copied(), before));
                }
                ("until" | "times", [arg]) => {
                    ensure!(end.is_none(), "repeat() accepts one until() or times()");
                    end = Some((step.name, *arg, before));
                }
                _ => bail!("invalid repeat modulator {}", step.name),
            }
            Ok(())
        };
        let modifier = |step: &Step<'_>| matches!(step.name, "until" | "times" | "emit");
        let mut body = first;
        while modifier(body) {
            modulate(body, true)?;
            body = steps.next().context("repeat modifier requires repeat()")?;
        }
        let ("repeat", [body]) = (body.name, body.args.as_slice()) else {
            bail!("repeat() expects one traversal");
        };
        let body = parse_chain(body, "__")?;
        ensure!(!body.is_empty(), "repeat() requires steps");
        while let Some(step) = steps.next_if(|step| modifier(step)) {
            modulate(step, false)?;
        }
        let emit_sql = emit
            .and_then(|(arg, _)| arg)
            .map(|arg| super::values::by_traversal(arg).context("emit() expects a traversal"))
            .transpose()?;
        let emit_steps = emit_sql
            .as_deref()
            .map(|arg| parse_chain(arg, "__"))
            .transpose()?;
        let emit = emit.map(|(_, before)| Emit {
            predicate: emit_steps.as_deref().unwrap_or_default(),
            before,
        });
        let mut tail = steps.clone();
        let mut productive = Vec::new();
        while let Some(step) = tail.next_if(|step| {
            productive_step(step)
                || matches!(step.name, "where" | "filter" | "project")
                || step.name == "by"
                    && productive
                        .iter()
                        .any(|step: &Step<'_>| step.name == "project")
        }) {
            productive.push(step.clone());
        }
        let dedup_at = tail
            .next_if(|step| step.name == "dedup" && step.args.is_empty())
            .map(|_| productive.len());
        if dedup_at.is_some() {
            while let Some(step) = tail.next_if(|step| productive_step(step)) {
                productive.push(step.clone());
            }
        }
        let limit = if emit.is_some() {
            let mut offset = 0_u64;
            while let Some(step) = tail.next_if(|step| step.name == "skip" && step.args.len() == 1)
            {
                offset = offset.saturating_add(super::integer(step.args[0])?);
            }
            let count = match tail.peek().map(|step| (step.name, step.args.as_slice())) {
                Some(("limit", [count])) => Some(super::integer(count)?),
                Some(("range", [low, high])) => super::parse::range_bounds(low, high)?.1,
                _ => None,
            }
            .map(|count| {
                if count == 0 {
                    0
                } else {
                    offset.saturating_add(count)
                }
            });
            count.map(|count| EmissionTail {
                limit: OutputLimit {
                    count,
                    group: self.group.as_ref().map(|group| group.key.clone()),
                    distinct: None,
                },
                productive,
                dedup_at,
            })
        } else {
            None
        };
        match end {
            Some(("times", arg, before)) => {
                let times = super::integer(arg)?;
                ensure!(
                    times <= super::MAX_REPEAT_ROUNDS,
                    "repeat() is limited to 256 iterations"
                );
                let rounds = if before { times } else { times.max(1) };
                if emit.is_none() && !traversal_contains(&body, depth + 1, barrier)? {
                    for _ in 0..rounds {
                        self.apply(&body, depth + 1)?;
                    }
                    Ok(())
                } else {
                    self.compile_repeat(&body, LoopEnd::Times(rounds), !before, emit, limit, depth)
                }
            }
            Some(("until", arg, before)) => {
                let traversal =
                    super::values::by_traversal(arg).context("until() expects a traversal")?;
                let predicate = parse_chain(&traversal, "__")?;
                ensure!(!predicate.is_empty(), "until() requires a predicate");
                if emit.is_none()
                    && simple_check(&predicate)
                    && self.try_recursive_repeat(&body, &predicate, !before, depth)?
                {
                    Ok(())
                } else {
                    self.compile_repeat(
                        &body,
                        LoopEnd::Until(&predicate),
                        !before,
                        emit,
                        limit,
                        depth,
                    )
                }
            }
            None if emit.is_some() => {
                self.compile_repeat(&body, LoopEnd::Exhausted, true, emit, limit, depth)
            }
            None => bail!("repeat() requires times(), until() or emit()"),
            _ => unreachable!("repeat end modulator"),
        }
    }

    fn compile_repeat(
        &mut self,
        steps: &[Step<'_>],
        end: LoopEnd<'_, '_>,
        after: bool,
        emit: Option<Emit<'_, '_>>,
        limit: Option<EmissionTail<'_>>,
        depth: usize,
    ) -> Result<()> {
        ensure!(depth < 16, "Gremlin nesting exceeds 16");
        let table = self.fresh("repeat");
        let seed = self.fresh("work");
        let seen = self.fresh("until");
        let tracked = if emit.is_some()
            || matches!(end, LoopEnd::Times(_))
            || !self.order.is_empty()
            || traversal_contains(steps, depth + 1, |name| name == "order")?
        {
            Some((self.fresh("round"), self.fresh("position")))
        } else {
            None
        };
        let mut work = self.clone();
        if has_barrier(steps) {
            work.begin_iteration();
        }
        self.next_column = work.next_column;
        if let Some((iteration, position)) = &tracked {
            let order = work.sequence_order();
            work.sql = format!(
                "SELECT v.*, CAST(0 AS BIGINT) AS {}, row_number() OVER (ORDER BY {order} ROWS UNBOUNDED PRECEDING) AS {} FROM ({}) AS v",
                ident(iteration),
                ident(position),
                work.sql
            );
            work.order = vec![(position.clone(), false)];
            work.control_columns.push(iteration.clone());
        }
        let mut kinds = self.kinds();
        let mut tags = self.tags.clone();
        // The work table schema covers every reachable output kind, including kinds
        // reached only after the first iteration.
        let mut step = loop {
            let mut step = work.clone();
            step.adopt(kinds.clone(), tags.clone());
            step.sql = format!("SELECT * FROM {}", ident(&seed));
            if let Some(group) = &mut step.group {
                group.input = step.sql.clone();
            }
            if kinds.len() == 1 {
                step = step.lanes(&seed).pop().expect("single work kind");
            }
            step.ctes.clear();
            step.repeats = Rc::new(RefCell::new(Vec::new()));
            step.apply(steps, depth + 1)?;
            let merged = kinds
                .iter()
                .cloned()
                .chain(step.kinds())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let mut merged_tags = tags.clone();
            extend_tags(&mut merged_tags, &step.tags);
            if merged == kinds && merged_tags == tags {
                break step;
            }
            ensure!(
                merged.len() <= 128,
                "repeat() has more than 128 traverser kinds"
            );
            kinds = merged;
            tags = merged_tags;
        };
        let initial = work.packed(&kinds, &tags, true);
        let initial = if self.ctes.is_empty() {
            initial
        } else {
            format!("WITH {} {initial}", self.ctes.join(", "))
        };
        if let Some((iteration, position)) = &tracked {
            let order = step.sequence_order();
            step.order.clear();
            step.control_columns.retain(|name| name != iteration);
            step.sql = format!(
                "SELECT {}, v.{} + 1 AS {}, row_number() OVER (ORDER BY {order} ROWS UNBOUNDED PRECEDING) AS {} FROM ({}) AS v",
                step.select_columns("v"),
                ident(iteration),
                ident(iteration),
                ident(position),
                step.sql
            );
            step.control_columns.clone_from(&work.control_columns);
            step.order.clone_from(&work.order);
        }
        let body = step.packed(&kinds, &tags, true);
        self.labels.append(&mut step.labels);
        self.next_column = self.next_column.max(step.next_column);
        work.adopt(kinds.clone(), tags.clone());
        self.adopt(kinds.clone(), tags.clone());
        let mut terminal = Vec::new();
        let (mut checked, mut checked_nested) = match end {
            LoopEnd::Exhausted => (
                format!(
                    "SELECT v.*, FALSE AS {} FROM {} AS v",
                    ident(&seen),
                    ident(&seed)
                ),
                Vec::new(),
            ),
            LoopEnd::Times(rounds) => {
                terminal = step.kinds();
                let (round, _) = tracked.as_ref().expect("round counter");
                (
                    format!(
                        "SELECT v.*, v.{} >= {rounds} AS {} FROM {} AS v",
                        ident(round),
                        ident(&seen),
                        ident(&seed)
                    ),
                    Vec::new(),
                )
            }
            LoopEnd::Until(predicate) if simple_check(predicate) => {
                let mut checks = Vec::new();
                for mut lane in work.lanes(&seed) {
                    let (test, possible) = lane.loop_predicate(predicate)?;
                    if possible {
                        terminal.extend(lane.kinds());
                    }
                    lane.sql = format!(
                        "SELECT v.*, coalesce(({test}), FALSE) AS {} FROM ({}) AS v",
                        ident(&seen),
                        lane.sql
                    );
                    lane.control_columns.push(seen.clone());
                    checks.push(lane.packed(&kinds, &tags, true));
                }
                (checks.join(" UNION ALL "), Vec::new())
            }
            LoopEnd::Until(predicate) => self.predicate_check(
                &work,
                &format!("SELECT * FROM {}", ident(&seed)),
                predicate,
                (&seen, None),
                depth,
            )?,
        };
        let emit_before = emit.as_ref().is_some_and(|emit| emit.before);
        let emitted = if let Some(emit) = emit {
            let flag = self.fresh("emitted");
            if emit.predicate.is_empty() {
                checked = format!("SELECT v.*, TRUE AS {} FROM ({checked}) AS v", ident(&flag));
            } else {
                let (sql, nested) =
                    self.predicate_check(&work, &checked, emit.predicate, (&flag, None), depth)?;
                checked = sql;
                checked_nested.extend(nested);
            }
            terminal.clone_from(&kinds);
            Some(flag)
        } else {
            None
        };
        let (limit, eligible, post_eligible) = if let Some(EmissionTail {
            mut limit,
            productive,
            dedup_at,
        }) = limit
        {
            let distinct = dedup_at.is_some();
            let prefix = &productive[..dedup_at.unwrap_or(productive.len())];
            let eligible = if distinct
                || prefix
                    .iter()
                    .any(|step| !matches!(step.name, "identity" | "path"))
            {
                // Count the downstream survivors, while keeping the original
                // traversers for the next round and for the actual tail.
                let flag = self.fresh("eligible");
                limit.distinct = distinct.then(|| self.fresh("quota_key"));
                let (sql, nested) = self.predicate_check(
                    &work,
                    &checked,
                    prefix,
                    (&flag, limit.distinct.as_deref()),
                    depth,
                )?;
                checked = sql;
                checked_nested.extend(nested);
                Some(flag)
            } else {
                None
            };
            let post_eligible = if prefix.len() < productive.len() {
                let flag = self.fresh("post_dedup");
                let (sql, nested) =
                    self.predicate_check(&work, &checked, &productive, (&flag, None), depth)?;
                checked = sql;
                checked_nested.extend(nested);
                Some(flag)
            } else {
                None
            };
            (Some(limit), eligible, post_eligible)
        } else {
            (None, None, None)
        };
        if limit.is_some() {
            let (iteration, position) = tracked.as_ref().expect("emitting loop is ordered");
            // The outer limit consumes this order, so only discard later rows.
            checked = format!(
                "SELECT v.* FROM ({checked}) AS v ORDER BY {}, {}",
                qualified("v", iteration),
                qualified("v", position)
            );
        }
        ensure!(
            [&initial, &body, &checked]
                .into_iter()
                .all(|sql| sql.len() <= 256 * 1024),
            "Gremlin repeat plan exceeds 256 KiB"
        );
        self.repeats.borrow_mut().push(Repeat {
            table: table.clone(),
            seed,
            seen,
            initial,
            body,
            checked,
            after,
            emitted,
            eligible,
            post_eligible,
            emit_before,
            limit,
            nested: step.repeats.take(),
            checked_nested,
        });
        self.sql = format!("SELECT * FROM {}", ident(&table));
        if let Some((iteration, position)) = tracked {
            self.order = vec![(position, false), (iteration, false)];
        }
        if let [kind] = terminal.as_slice() {
            let index = kinds
                .iter()
                .position(|candidate| candidate == kind)
                .expect("terminal kind");
            let lane = self.lanes(&table).swap_remove(index);
            self.entity = lane.entity;
            self.scalar_kind = lane.scalar_kind;
            self.variants.clear();
            self.sql = lane.sql;
        }
        Ok(())
    }

    fn predicate_check(
        &mut self,
        work: &Self,
        input: &str,
        predicate: &[Step<'_>],
        (flag, distinct): (&str, Option<&str>),
        depth: usize,
    ) -> Result<(String, Vec<Repeat>)> {
        let table = self.fresh("predicate");
        let row = self.fresh("predicate_row");
        let cte = format!(
            "{} AS (SELECT v.*, row_number() OVER (ORDER BY {} ROWS UNBOUNDED PRECEDING) AS {} FROM ({input}) AS v)",
            ident(&table),
            work.sequence_order(),
            ident(&row)
        );
        let mut check = work.clone();
        check.next_column = self.next_column;
        check.ctes = vec![cte.clone()];
        check.repeats = Rc::default();
        check.sql = format!("SELECT * FROM {}", ident(&table));
        check.enter_branch(&row);
        check.apply(predicate, depth + 1)?;
        self.labels.append(&mut check.labels);
        self.next_column = check.next_column;
        let table = ident(&table);
        let row = ident(&row);
        let flag = ident(flag);
        let (key, output) = if let Some(distinct) = distinct {
            let mut fields = check.dedup_keys("v");
            if let Some(group) = &work.group {
                fields.push(qualified("v", &group.key));
            }
            let fields = fields
                .into_iter()
                .enumerate()
                .flat_map(|(i, value)| [format!("'k{i}'"), value])
                .collect::<Vec<_>>()
                .join(", ");
            let distinct = ident(distinct);
            (
                format!(", named_struct({fields}) AS {distinct}"),
                format!(", m.{distinct}"),
            )
        } else {
            (String::new(), String::new())
        };
        let sql = format!(
            "WITH {cte} SELECT v.*, m.{row} IS NOT NULL AS {flag}{output} FROM {table} AS v
             LEFT JOIN (SELECT DISTINCT {row}{key} FROM ({}) AS v) AS m ON v.{row} = m.{row}",
            check.sql
        );
        Ok((sql, check.repeats.take()))
    }

    fn try_recursive_repeat(
        &mut self,
        steps: &[super::Step<'_>],
        predicate: &[super::Step<'_>],
        after: bool,
        depth: usize,
    ) -> Result<bool> {
        // DataFusion accepts one work-table scan. Keep that streaming path for
        // single-shape loops whose steps cannot create competing scans.
        if !self.variants.is_empty()
            || has_barrier(steps)
            || steps.iter().any(|step| {
                matches!(
                    step.name,
                    "union"
                        | "coalesce"
                        | "choose"
                        | "repeat"
                        | "until"
                        | "select"
                        | "where"
                        | "emit"
                ) || (self.catalog.edges.len() > 1
                    && step.args.is_empty()
                    && matches!(step.name, "out" | "in" | "both" | "outE" | "inE" | "bothE"))
            })
        {
            return Ok(false);
        }
        let table = self.fresh("repeat");
        let seen = self.fresh("until");
        let marker = ident(&seen);
        let (condition, _) = self.loop_predicate(predicate)?;
        let condition = if after {
            format!("{marker} > 0 AND ({condition})")
        } else {
            condition
        };
        let mut step = self.clone();
        step.control_columns.push(seen);
        step.sql = format!(
            "SELECT v.* FROM {} AS v WHERE NOT coalesce(({condition}), FALSE)",
            ident(&table)
        );
        if step.apply(steps, depth + 1).is_err()
            || !step.variants.is_empty()
            || step.kinds() != self.kinds()
            || step.tags != self.tags
        {
            return Ok(false);
        }
        self.labels.extend(step.labels);
        self.next_column = self.next_column.max(step.next_column);
        let columns = self.select_columns("v");
        self.sql = format!(
            "WITH RECURSIVE {} AS (SELECT {columns}, CAST(0 AS BIGINT) AS {marker} FROM ({}) AS v
             UNION ALL SELECT {columns}, __graphlake_gremlin_repeat_depth(v.{marker} + 1) AS {marker}
             FROM ({}) AS v) SELECT {columns} FROM {} AS v WHERE {condition}",
            ident(&table),
            self.sql,
            step.sql,
            ident(&table)
        );
        Ok(true)
    }

    fn loop_predicate(&self, predicate: &[super::Step<'_>]) -> Result<(String, bool)> {
        let mut conditions = Vec::new();
        let mut possible = true;
        for step in predicate {
            let test = match (step.name, step.args.as_slice()) {
                ("hasLabel", [label]) => {
                    let label = text(label)?;
                    self.entity
                        .as_ref()
                        .is_some_and(|entity| entity.label == label)
                        .to_string()
                }
                ("has", [field]) | ("has", [field, _]) | ("has", [_, field, _]) => {
                    let field = text(field)?;
                    let label_matches = if let [label, _, _] = step.args.as_slice() {
                        let label = text(label)?;
                        self.entity
                            .as_ref()
                            .is_some_and(|entity| entity.label == label)
                    } else {
                        true
                    };
                    if !label_matches
                        || !self
                            .entity
                            .as_ref()
                            .is_some_and(|entity| entity.fields.contains(&field))
                    {
                        "FALSE".into()
                    } else if let [_, value] | [_, _, value] = step.args.as_slice() {
                        super::has_condition(&qualified("v", &field), value)?
                    } else {
                        format!("{} IS NOT NULL", qualified("v", &field))
                    }
                }
                ("is", [value]) if self.entity.is_none() => condition("v.value", value)?,
                ("is", [_]) => "FALSE".into(),
                ("simplePath", []) if self.path => {
                    format!("array_length({PATH}) = array_length(array_distinct({PATH}))")
                }
                _ => bail!("until() supports property, label, scalar and simplePath predicates"),
            };
            possible &= !test.eq_ignore_ascii_case("FALSE");
            conditions.push(format!("({test})"));
        }
        Ok((conditions.join(" AND "), possible))
    }
}

fn simple_check(steps: &[Step<'_>]) -> bool {
    steps
        .iter()
        .all(|step| matches!(step.name, "has" | "hasLabel" | "is" | "simplePath"))
}

fn traversal_contains(steps: &[Step<'_>], depth: usize, test: fn(&str) -> bool) -> Result<bool> {
    ensure!(depth <= 16, "Gremlin nesting exceeds 16");
    for step in steps {
        if test(step.name) {
            return Ok(true);
        }
        if matches!(
            step.name,
            "repeat"
                | "union"
                | "coalesce"
                | "choose"
                | "until"
                | "where"
                | "filter"
                | "not"
                | "map"
                | "emit"
        ) {
            for arg in &step.args {
                let arg = super::values::by_traversal(arg).context("expected nested traversal")?;
                if traversal_contains(&parse_chain(&arg, "__")?, depth + 1, test)? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
