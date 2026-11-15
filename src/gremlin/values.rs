use std::{collections::BTreeSet, iter::Peekable, slice::Iter};

use anyhow::{Context, Result, bail, ensure};
use opencypher::ExprKind;

use super::parse::parse_expr;

use super::{
    Step, Traversers,
    control::Group,
    ident, parse_chain, qualified, string, text,
    variants::{KIND, Kind, slot, tag_column},
};

pub(super) fn modulators<'q>(
    steps: &mut Peekable<Iter<'_, Step<'q>>>,
) -> Result<Vec<Option<&'q str>>> {
    let mut output = Vec::new();
    while let Some(step) = steps.next_if(|step| step.name == "by") {
        output.push(match step.args.as_slice() {
            [] => None,
            [arg] => Some(*arg),
            _ => bail!("by() expects a property or one traversal"),
        });
    }
    Ok(output)
}

pub(super) fn by_traversal(arg: &str) -> Option<String> {
    if arg.starts_with(['\'', '"']) {
        None
    } else if arg.starts_with("__.") || arg == "__" {
        Some(arg.to_owned())
    } else {
        Some(format!("__.{arg}"))
    }
}

fn empty_array(kind: &Kind, alias: &str) -> String {
    format!(
        "array_slice(make_array({}), 1, 0)",
        kind.payload_null(alias)
    )
}

pub(super) fn empty_map(kind: &Kind, alias: &str) -> String {
    format!(
        "map({}, {})",
        empty_array(&Kind::Label, alias),
        empty_array(kind, alias)
    )
}

fn record_entries(fields: Vec<(String, Kind)>) -> (String, Kind) {
    let kinds = fields
        .iter()
        .map(|(_, kind)| kind.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let value_kind = match kinds.as_slice() {
        [] => Kind::Null,
        [kind] => kind.clone(),
        _ => Kind::Variant(kinds.clone()),
    };
    let entry_kind = Kind::Record(vec![
        ("key".into(), Kind::Label),
        ("value".into(), value_kind),
    ]);
    if fields.is_empty() {
        return (empty_array(&entry_kind, "v"), entry_kind);
    }
    let entries = fields.into_iter().map(|(name, kind)| {
        let name = string(&name);
        let item = format!("get_field(v.value, {name})");
        let value = if kinds.len() == 1 {
            item.clone()
        } else {
            let index = kinds.iter().position(|candidate| *candidate == kind).expect("field kind");
            let mut fields = vec!["'kind'".into(), format!("CAST({index} AS BIGINT)")];
            for (slot, candidate) in kinds.iter().enumerate() {
                let value = if slot != index {
                    candidate.null_slot("v")
                } else if matches!(kind, Kind::Element(_)) {
                    format!("get_field({item}, 'key')")
                } else {
                    item.clone()
                };
                fields.extend([string(&format!("value_{slot}")), value]);
            }
            format!("named_struct({})", fields.join(", "))
        };
        format!("CASE WHEN {item} IS NOT NULL THEN named_struct('key', {name}, 'value', {value}) END")
    }).collect::<Vec<_>>();
    (format!("make_array({})", entries.join(", ")), entry_kind)
}

impl Traversers<'_> {
    fn payload(&self, alias: &str) -> (String, Kind) {
        if let Some(entity) = &self.entity {
            (entity.identity(alias), Kind::Element(entity.clone()))
        } else if self.variants.is_empty() {
            (qualified(alias, "value"), self.scalar_kind.clone())
        } else {
            let mut fields = vec!["'kind'".into(), qualified(alias, KIND)];
            fields.extend(self.variants.iter().enumerate().flat_map(|(index, _)| {
                [
                    string(&format!("value_{index}")),
                    qualified(alias, &slot(index)),
                ]
            }));
            (
                format!("named_struct({})", fields.join(", ")),
                Kind::Variant(self.variants.clone()),
            )
        }
    }

    fn restore_payload(&mut self, kind: Kind) {
        self.entity = None;
        self.variants.clear();
        let value = qualified("v", "value");
        match kind {
            Kind::Element(entity) => {
                self.sql = self.hydrate(&entity, &value, &self.sql, true);
                self.entity = Some(entity);
            }
            Kind::Variant(kinds) => {
                let mut fields = vec![format!("get_field({value}, 'kind') AS {KIND}")];
                fields.extend(kinds.iter().enumerate().map(|(index, _)| {
                    format!(
                        "get_field({value}, 'value_{index}') AS {}",
                        ident(&slot(index))
                    )
                }));
                self.sql = format!(
                    "SELECT {}{} FROM ({}) AS v",
                    fields.join(", "),
                    self.carry("v", None),
                    self.sql
                );
                self.variants = kinds;
            }
            kind => self.scalar_kind = kind,
        }
    }

    pub(super) fn fold(&mut self) {
        let (value, kind) = self.payload("v");
        let order = self.order_clause();
        self.reduce(
            &format!("array_agg({value}{order})"),
            &format!("coalesce(q.aggregate, {})", empty_array(&kind, "q")),
            Kind::List(Box::new(kind)),
        );
    }

    pub(super) fn unfold(&mut self) {
        if self.entity.is_some() {
            return;
        }
        let (items, kind) = match self.scalar_kind.clone() {
            Kind::List(kind) => ("v.value".into(), *kind),
            Kind::Path | Kind::Property(..) | Kind::Item(_) => (
                "__graphlake_gremlin_items(v.value)".into(),
                Kind::Item(Box::new(self.scalar_kind.clone())),
            ),
            Kind::Group(kind) => (
                "__graphlake_gremlin_items(v.value)".into(),
                Kind::Record(vec![("key".into(), Kind::Label), ("value".into(), *kind)]),
            ),
            Kind::Record(fields) => record_entries(fields),
            Kind::Map(entity, fields) => {
                let fields = fields
                    .into_iter()
                    .filter(|field| entity.fields.contains(field))
                    .map(|field| {
                        let value = Kind::Property(entity.clone(), field.clone());
                        let kind = if entity.edge {
                            value
                        } else {
                            Kind::List(Box::new(value))
                        };
                        (field, kind)
                    })
                    .collect();
                record_entries(fields)
            }
            _ => return,
        };
        let index = self.fresh("unfold_index");
        self.sql = format!(
            "SELECT unnest({items}) AS value, unnest(range(0, CAST(array_length({items}) AS BIGINT))) AS {}{}
             FROM ({}) AS v",
            ident(&index), self.carry("v", None), self.sql
        );
        self.order.insert(0, (index, false));
        if matches!(self.scalar_kind, Kind::Record(_) | Kind::Map(..)) {
            self.restrict("v.value IS NOT NULL");
        }
        self.restore_payload(kind);
    }

    pub(super) fn where_values(&mut self, args: &[&str], depth: usize) -> Result<()> {
        let (tag, nested) = match args {
            [nested] => (None, *nested),
            [tag, predicate] => (Some(text(tag)?), *predicate),
            _ => bail!("where() expects a traversal or a label predicate"),
        };
        if let Ok(expression) = parse_expr(nested)
            && let ExprKind::FunctionCall {
                name,
                args,
                distinct: false,
            } = expression.kind
            && let Some(op) = match name.0.trim_start_matches("P.") {
                "eq" => Some("="),
                "neq" => Some("<>"),
                _ => None,
            }
            && let [value] = args.as_slice()
            && let ExprKind::String(other) = &value.kind
        {
            let tag_value = |name: &str| -> Result<String> {
                ensure!(self.tags.contains_key(name), "unknown step label {name}");
                Ok(qualified("v", &tag_column(name)))
            };
            let left = if let Some(tag) = tag {
                tag_value(&tag)?
            } else if let Some(entity) = &self.entity {
                entity.identity("v")
            } else {
                let arms = self
                    .variants
                    .iter()
                    .enumerate()
                    .filter_map(|(index, kind)| {
                        let Kind::Element(entity) = kind else {
                            return None;
                        };
                        Some(format!(
                            "WHEN {index} THEN named_struct('label', {}, 'kind', '{}', 'key', {})",
                            string(&entity.label),
                            if entity.edge { "edge" } else { "vertex" },
                            qualified("v", &slot(index))
                        ))
                    })
                    .collect::<Vec<_>>();
                ensure!(
                    !arms.is_empty(),
                    "where() label comparison expects graph elements"
                );
                format!("CASE v.{KIND} {} END", arms.join(" "))
            };
            self.restrict(&format!("{left} {op} {}", tag_value(other)?));
            return Ok(());
        }
        ensure!(
            tag.is_none(),
            "where() label predicates support eq() and neq()"
        );
        self.filter("where", nested, depth)
    }

    pub(super) fn filter(&mut self, mode: &str, nested: &str, depth: usize) -> Result<()> {
        let traversal =
            by_traversal(nested).with_context(|| format!("{mode}() expects a traversal"))?;
        let mut steps = parse_chain(&traversal, "__")?;
        ensure!(
            !steps.is_empty() || mode == "not",
            "{mode}() expects a traversal"
        );
        steps.push(Step {
            name: "limit",
            args: vec!["1"],
        });
        let (table, row, cte) = self.seed_rows(2)?;
        let mut matches = self.per_row(&table, &row, &cte);
        matches.apply(&steps, depth + 1)?;
        self.absorb(&matches);
        let join = if mode == "not" {
            "LEFT ANTI JOIN"
        } else {
            "LEFT SEMI JOIN"
        };
        self.sql = format!(
            "WITH {cte} SELECT {} FROM {table} AS v
             {join} (SELECT {row} FROM ({}) AS matches) AS m ON v.{row} = m.{row}",
            self.select_columns("v"),
            matches.sql,
            row = ident(&row)
        );
        Ok(())
    }

    pub(super) fn modulate(&mut self, by: Option<&str>, depth: usize) -> Result<(bool, bool)> {
        let Some(arg) = by else {
            return Ok((false, false));
        };
        if let Some(traversal) = by_traversal(arg) {
            let steps = parse_chain(&traversal, "__")?;
            let reducer = |step: &Step<'_>| {
                matches!(
                    step.name,
                    "count" | "fold" | "group" | "groupCount" | "mean" | "sum" | "min" | "max"
                )
            };
            let last = steps
                .iter()
                .rfind(|step| step.name != "by")
                .is_some_and(reducer);
            let reduces = steps.iter().any(reducer);
            self.apply(&steps, depth + 1)?;
            Ok((reduces, last))
        } else {
            let field = text(arg)?;
            if self
                .kinds()
                .iter()
                .any(|kind| matches!(kind, Kind::Element(entity) if entity.fields.contains(&field)))
            {
                self.apply(
                    &[Step {
                        name: "values",
                        args: vec![arg],
                    }],
                    depth + 1,
                )?;
            } else {
                self.scalar("NULL", true, Kind::Null);
                self.variants.clear();
            }
            Ok((false, false))
        }
    }

    fn select_one(&mut self, name: &str) -> Result<()> {
        if self.entity.is_none() && self.variants.is_empty() {
            let field = match &self.scalar_kind {
                Kind::Record(fields) => {
                    fields
                        .iter()
                        .find(|(field, _)| field == name)
                        .map(|(_, kind)| {
                            (
                                format!("get_field(v.value, {})", string(name)),
                                kind.clone(),
                            )
                        })
                }
                Kind::Group(kind) => Some((
                    format!("array_element(map_extract(v.value, {}), 1)", string(name)),
                    *kind.clone(),
                )),
                _ => None,
            };
            if let Some((expression, kind)) = field {
                self.sql = format!(
                    "SELECT {expression} AS value{} FROM ({}) AS v WHERE {expression} IS NOT NULL",
                    self.carry("v", None),
                    self.sql
                );
                self.restore_payload(kind);
                return Ok(());
            }
        }
        self.select_tag(name)
    }

    pub(super) fn project_values(
        &mut self,
        fields: &[&str],
        by: Vec<Option<&str>>,
        depth: usize,
    ) -> Result<()> {
        self.record_values(fields, &by, false, false, depth)
    }

    pub(super) fn select_values(
        &mut self,
        fields: &[&str],
        by: Vec<Option<&str>>,
        depth: usize,
    ) -> Result<()> {
        if let [field] = fields {
            ensure!(
                by.len() <= 1,
                "select() has more by() modulators than fields"
            );
            let field = text(field)?;
            if by.first().is_some_and(Option::is_some) {
                self.record_values(fields, &by, true, false, depth)?;
            }
            return self.select_one(&field);
        }
        self.record_values(fields, &by, true, false, depth)
    }

    fn seed_rows(&mut self, references: usize) -> Result<(String, String, String)> {
        // The SQL planner inlines this CTE once per reference. A small SQL
        // string can otherwise produce an exponentially large logical plan.
        let copies = self.inline_copies.get().saturating_mul(references);
        ensure!(
            copies <= 256,
            "Gremlin traversal exceeds the correlated plan size limit"
        );
        self.inline_copies.set(copies);
        let table = ident(&self.fresh("seed"));
        let row = self.fresh("row");
        let cte = format!(
            "{table} AS (SELECT v.*, row_number() OVER (ORDER BY {} ROWS UNBOUNDED PRECEDING) AS {} FROM ({}) AS v)",
            self.sequence_order(),
            ident(&row),
            self.sql
        );
        Ok((table, row, cte))
    }

    fn per_row(&self, table: &str, row: &str, cte: &str) -> Self {
        let mut state = self.clone();
        state.sql = format!("SELECT * FROM {table}");
        state.ctes.push(cte.into());
        state.enter_branch(row);
        state
    }

    fn absorb(&mut self, child: &Self) {
        self.next_column = self.next_column.max(child.next_column);
        self.labels.extend(child.labels.iter().cloned());
    }

    pub(super) fn record_values(
        &mut self,
        fields: &[&str],
        by: &[Option<&str>],
        select: bool,
        include_current: bool,
        depth: usize,
    ) -> Result<()> {
        ensure!(!fields.is_empty(), "project() requires fields");
        ensure!(by.len() <= fields.len(), "more by() modulators than fields");
        let names = fields
            .iter()
            .map(|name| text(name))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            names.iter().collect::<BTreeSet<_>>().len() == names.len(),
            "duplicate output field"
        );
        let (table, row, cte) = self.seed_rows(names.len() + 1)?;
        let mut joins = Vec::new();
        let mut items = Vec::new();
        let mut fields = Vec::new();
        for (index, name) in names.into_iter().enumerate() {
            let mut child = self.per_row(&table, &row, &cte);
            if select && !(include_current && name == "_") {
                child.select_one(&name)?;
            }
            let modulator = by.iter().copied().cycle().nth(index).flatten();
            child.modulate(modulator, depth)?;
            let (payload, kind) = child.payload("v");
            let join = if select { "JOIN" } else { "LEFT JOIN" };
            joins.push(format!(
                "{join} (SELECT {}, {payload} AS value FROM ({}) AS v
                 QUALIFY row_number() OVER (PARTITION BY {} ORDER BY {} ROWS UNBOUNDED PRECEDING) = 1) AS p{index}
                 ON p{index}.{} = {}",
                ident(&row),
                child.sql,
                ident(&row),
                child.sequence_order(),
                ident(&row),
                qualified("v", &row),
            ));
            items.extend([string(&name), format!("p{index}.value")]);
            fields.push((name, kind));
            self.absorb(&child);
        }
        self.sql = format!(
            "WITH {cte} SELECT named_struct({}) AS value{} FROM {table} AS v {}",
            items.join(", "),
            self.carry("v", None),
            joins.join(" "),
        );
        self.restore_payload(Kind::Record(fields));
        Ok(())
    }

    fn group_key(&self) -> String {
        let element_key = |entity: &super::Entity, key: &str| {
            format!(
                "__graphlake_gremlin_key({})",
                crate::cypher::element_id(&string(&entity.label), key)
            )
        };
        if let Some(entity) = &self.entity {
            return element_key(entity, &crate::traversal::text_key("v", &entity.key));
        }
        if self.variants.is_empty() {
            return "__graphlake_gremlin_key(v.value)".into();
        }
        let arms = self
            .variants
            .iter()
            .enumerate()
            .map(|(index, kind)| {
                let value = qualified("v", &slot(index));
                let key = match kind {
                    Kind::Element(entity) => element_key(entity, &value),
                    _ => format!("__graphlake_gremlin_key({value})"),
                };
                format!("WHEN {index} THEN {key}")
            })
            .collect::<Vec<_>>()
            .join(" ");
        format!("CASE v.{KIND} {arms} END")
    }

    fn inline_group_key(&self, by: Option<&str>) -> Result<Option<String>> {
        let Some(arg) = by else {
            return Ok(Some(self.group_key()));
        };
        let Some(traversal) = by_traversal(arg) else {
            let field = text(arg)?;
            return Ok(self.entity.as_ref().map(|entity| {
                if entity.fields.contains(&field) {
                    format!("__graphlake_gremlin_key({})", qualified("v", &field))
                } else {
                    "CAST(NULL AS VARCHAR)".into()
                }
            }));
        };
        let steps = parse_chain(&traversal, "__")?;
        Ok(match steps.as_slice() {
            [] => Some(self.group_key()),
            [step] if step.args.is_empty() && step.name == "identity" => Some(self.group_key()),
            [step] if step.args.is_empty() && step.name == "label" => {
                if let Some(entity) = &self.entity {
                    Some(format!(
                        "__graphlake_gremlin_key({})",
                        string(&entity.label)
                    ))
                } else if !self.variants.is_empty() {
                    let arms = self
                        .variants
                        .iter()
                        .enumerate()
                        .filter_map(|(index, kind)| match kind {
                            Kind::Element(entity) => {
                                Some(format!("WHEN {index} THEN {}", string(&entity.label)))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    (!arms.is_empty()).then(|| {
                        format!(
                            "__graphlake_gremlin_key(CASE v.{KIND} {} END)",
                            arms.join(" ")
                        )
                    })
                } else {
                    None
                }
            }
            _ => None,
        })
    }

    fn group_property_rows(
        &mut self,
        by: Option<&str>,
        key: &str,
        ctes: &mut Vec<String>,
    ) -> Result<Option<String>> {
        let Some(arg) = by.filter(|arg| arg.starts_with(['\'', '"'])) else {
            return Ok(None);
        };
        let field = text(arg)?;
        let seed = ident(&self.fresh("group_property"));
        let reads = self
            .variants
            .iter()
            .enumerate()
            .filter_map(|(index, kind)| {
                let Kind::Element(entity) = kind else {
                    return None;
                };
                if !entity.fields.contains(&field) {
                    return None;
                }
                Some(format!(
                    "SELECT v.*, __graphlake_gremlin_key({}) AS {} FROM {seed} AS v
                 JOIN {} AS r ON {} = {} WHERE v.{KIND} = {index}",
                    qualified("r", &field),
                    ident(key),
                    ident(&entity.label),
                    crate::traversal::text_key("r", &entity.key),
                    qualified("v", &slot(index)),
                ))
            })
            .collect::<Vec<_>>();
        if reads.is_empty() {
            return Ok(Some(format!(
                "SELECT v.*, CAST(NULL AS VARCHAR) AS {} FROM ({}) AS v WHERE FALSE",
                ident(key),
                self.sql
            )));
        }
        ctes.push(format!("{seed} AS ({})", self.sql));
        Ok(Some(reads.join(" UNION ALL ")))
    }

    pub(super) fn group_values(&mut self, by: Vec<Option<&str>>, depth: usize) -> Result<()> {
        ensure!(by.len() <= 2, "group() accepts up to two by() modulators");
        let key = self.fresh("bucket_key");
        let group = self.fresh("bucket_id");
        let keyed = ident(&self.fresh("keyed"));
        let outer = self.group.as_ref().map_or_else(
            || "CAST(0 AS BIGINT)".into(),
            |group| qualified("v", &group.key),
        );
        let key_by = by.first().copied().flatten();
        let mut ctes = Vec::new();
        let keyed_input = if let Some(value) = self.inline_group_key(key_by)? {
            format!(
                "SELECT v.*, {value} AS {} FROM ({}) AS v",
                ident(&key),
                self.sql
            )
        } else if let Some(rows) = self.group_property_rows(key_by, &key, &mut ctes)? {
            rows
        } else {
            let (seed, row, seed_cte) = self.seed_rows(2)?;
            let mut keys = self.per_row(&seed, &row, &seed_cte);
            keys.modulate(key_by, depth)?;
            let value = keys.group_key();
            self.absorb(&keys);
            ctes.push(seed_cte);
            format!(
                "SELECT v.*, k.value AS {key}
                 FROM {seed} AS v JOIN (SELECT {row}, {value} AS value FROM ({keys}) AS v
                 QUALIFY row_number() OVER (PARTITION BY {row} ORDER BY {order} ROWS UNBOUNDED PRECEDING) = 1) AS k
                 ON v.{row} = k.{row}",
                key = ident(&key),
                row = ident(&row),
                keys = keys.sql,
                order = keys.sequence_order(),
            )
        };
        let key_field = qualified("v", &key);
        ctes.push(format!(
            "{keyed} AS (SELECT v.*, named_struct('scope', {outer}, 'key', {key_field}) AS {}
             FROM ({keyed_input}) AS v WHERE {key_field} IS NOT NULL)",
            ident(&group)
        ));
        let mut buckets = self.clone();
        buckets.sql = format!("SELECT * FROM {keyed}");
        buckets.ctes.extend(ctes.iter().cloned());
        buckets.control_columns.extend([key.clone(), group.clone()]);
        buckets.group = Some(Group {
            key: group.clone(),
            input: buckets.sql.clone(),
        });
        let (reduces, last_is_reducer) = buckets.modulate(by.get(1).copied().flatten(), depth)?;
        if !reduces {
            buckets.fold();
        }
        let first = if reduces && !last_is_reducer {
            format!(
                " QUALIFY row_number() OVER (PARTITION BY {} ORDER BY {} ROWS UNBOUNDED PRECEDING) = 1",
                qualified("v", &group),
                buckets.sequence_order()
            )
        } else {
            String::new()
        };
        let (payload, kind) = buckets.payload("v");
        let controls = self
            .control_columns
            .iter()
            .map(|name| format!(", {}", qualified("v", name)))
            .collect::<String>();
        self.sql = format!(
            "SELECT {key_field} AS bucket_key, {payload} AS bucket_value{controls} FROM ({}) AS v{first}",
            buckets.sql
        );
        self.absorb(&buckets);
        self.reduce(
            "__graphlake_gremlin_group_map(array_agg(named_struct('key', v.bucket_key, 'value', v.bucket_value)))",
            &format!("coalesce(q.aggregate, {})", empty_map(&kind, "q")),
            Kind::Group(Box::new(kind)),
        );
        self.sql = format!("WITH {} {}", ctes.join(", "), self.sql);
        Ok(())
    }
}
