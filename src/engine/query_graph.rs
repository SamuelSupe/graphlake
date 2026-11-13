//! Shapes of graph-valued result columns after the language's public projection.
use std::collections::BTreeMap;

#[derive(Default)]
pub(crate) struct GraphOutput {
    pub columns: BTreeMap<String, GraphColumn>,
    pub row_entity: Option<(String, bool)>,
    pub gremlin_variant: bool,
}

#[derive(Clone)]
pub(crate) enum GraphColumn {
    Entity {
        label: Option<String>,
        node: bool,
    },
    Path {
        labels: Option<(Option<String>, Option<String>)>,
    },
    List(Box<GraphColumn>),
    Record(BTreeMap<String, GraphColumn>),
}
