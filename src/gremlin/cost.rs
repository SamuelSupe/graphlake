use std::{collections::BTreeMap, ops::ControlFlow};

use datafusion::{
    common::{Result, plan_err},
    sql::{
        parser::Statement,
        sqlparser::ast::{ObjectName, Query, Statement as Sql, Visit, Visitor},
    },
};

const MAX_SCANS: usize = 512;

#[derive(Default)]
struct Frame {
    definitions: BTreeMap<*const Query, String>,
    visible: BTreeMap<String, usize>,
    scans: usize,
}

#[derive(Default)]
struct Expansion {
    stack: Vec<Frame>,
}

impl Visitor for Expansion {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        self.stack.push(Frame {
            definitions: query
                .with
                .iter()
                .flat_map(|with| &with.cte_tables)
                .map(|cte| {
                    (
                        cte.query.as_ref() as *const Query,
                        cte.alias.name.value.clone(),
                    )
                })
                .collect(),
            ..Default::default()
        });
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        let scans = self.stack.pop().unwrap().scans.max(1);
        if scans > MAX_SCANS {
            // DataFusion plans each CTE even when no later query reads it.
            return ControlFlow::Break(());
        }
        if let Some(parent) = self.stack.last_mut() {
            if let Some(name) = parent.definitions.remove(&(query as *const Query)) {
                parent.visible.insert(name, scans);
            } else {
                parent.scans = (parent.scans + scans).min(MAX_SCANS + 1);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        let cost = match relation.0.as_slice() {
            [part] => part.as_ident().and_then(|name| {
                self.stack
                    .iter()
                    .rev()
                    .find_map(|frame| frame.visible.get(&name.value).copied())
            }),
            _ => None,
        };
        if let Some(frame) = self.stack.last_mut() {
            frame.scans = (frame.scans + cost.unwrap_or(1)).min(MAX_SCANS + 1);
        }
        ControlFlow::Continue(())
    }
}

pub(crate) fn check(statement: &Statement) -> Result<()> {
    let sql = match statement {
        Statement::Explain(explain) => return check(&explain.statement),
        Statement::Statement(sql) => sql,
        _ => return Ok(()),
    };
    if let Sql::Query(query) = sql.as_ref() {
        // Charge each CTE when referenced, using the enclosing query's
        // lexical scope. Sibling branches add their costs.
        let mut expansion = Expansion::default();
        if stacker::maybe_grow(2 * 1024 * 1024, 8 * 1024 * 1024, || {
            query.visit(&mut expansion)
        })
        .is_break()
        {
            return plan_err!("Gremlin traversal exceeds the correlated plan size limit");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::parser::DFParser;

    #[test]
    fn unused_ctes_still_have_to_fit_the_planning_budget() -> Result<()> {
        let mut with = vec!["b0 AS (SELECT 1)".to_owned()];
        for i in 1..=10 {
            let prev = i - 1;
            with.push(format!(
                "b{i} AS (SELECT * FROM b{prev} UNION ALL SELECT * FROM b{prev})"
            ));
        }
        let sql = format!("WITH {} SELECT 1", with.join(","));
        let statements = DFParser::parse_sql(&sql)?;
        assert!(check(&statements[0]).is_err());
        Ok(())
    }
}
