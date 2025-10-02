use super::*;

use std::borrow::Cow;
use strum::{Display, EnumString};

/// Represents a paginated result set from a query.
///
/// This struct holds a collection of items (T) retrieved in the current request
/// and a cursor used to fetch the subsequent page of items.
pub struct QueryResult<T> {
    /// The collection of data items returned for this page.
    pub items: Vec<T>,
    /// The opaque cursor byte array representing the end position of the current
    /// result set. This cursor should be used in the next query request to
    /// continue pagination.
    pub end_cursor: Option<Vec<u8>>,
}

impl<T> QueryResult<T> {
    /// Creates a new `QueryResult` instance.
    pub fn new(items: Vec<T>, end_cursor: Option<Vec<u8>>) -> Self {
        QueryResult { items, end_cursor }
    }
}

impl From<google_datastore1::api::QueryResultBatch> for QueryResult<Entity> {
    fn from(value: google_datastore1::api::QueryResultBatch) -> Self {
        let end_cursor = value.end_cursor;
        let items = value
            .entity_results
            .unwrap_or_default()
            .into_iter()
            .map(|e| e.entity.expect("EntityResult without an entity").into())
            .collect();
        Self { items, end_cursor }
    }
}

#[derive(Clone, Debug)]
pub enum Filter {
    Composite(CompositeFilterOperator, Vec<Filter>),
    Property(Cow<'static, str>, FilterOperator, Value),
}

impl Filter {
    pub fn and(filters: Vec<Filter>) -> Option<Filter> {
        if filters.is_empty() {
            None
        } else if filters.len() == 1 {
            filters.into_iter().next()
        } else {
            Some(Filter::Composite(CompositeFilterOperator::And, filters))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CompositeFilterOperator {
    And,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum FilterOperator {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    In,
    NotEqual,
    HasAncestor,
    NotIn,
}

impl Into<google_datastore1::api::Filter> for Filter {
    fn into(self) -> google_datastore1::api::Filter {
        match self {
            Filter::Composite(op, filters) => google_datastore1::api::Filter {
                composite_filter: Some(google_datastore1::api::CompositeFilter {
                    filters: Some(filters.into_iter().map(|e| e.into()).collect()),
                    op: Some(op.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Filter::Property(name, op, value) => google_datastore1::api::Filter {
                property_filter: Some(google_datastore1::api::PropertyFilter {
                    op: Some(op.to_string()),
                    property: Some(google_datastore1::api::PropertyReference {
                        name: Some(name.into_owned())
                    }),
                    value: Some(value.into()),
                }),
                ..Default::default()
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderDirection {
    ASCENDING,
    DESCENDING,
}

#[derive(Clone, Debug)]
pub struct PropertyOrder {
    pub name: Cow<'static, str>,
    pub direction: OrderDirection,
}

impl PropertyOrder {
    pub fn new(name: Cow<'static, str>, direction: OrderDirection) -> Self {
        Self { name, direction }
    }
}

impl Into<google_datastore1::api::PropertyOrder> for PropertyOrder {
    fn into(self) -> google_datastore1::api::PropertyOrder {
        google_datastore1::api::PropertyOrder {
            property: Some(google_datastore1::api::PropertyReference { name: Some(self.name.into_owned()) }),
            direction: Some(self.direction.to_string()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug)]
pub struct Query {
    /// The kind on which the query is going to be performed.
    /// Use an empty string for kindless queries.
    /// The API supports 1 kind at most.
    pub kind: Cow<'static, str>,
    /// The filter to use on the entities
    pub filter: Option<Filter>,
    /// The cursor at which the query should begin.
    pub start_cursor: Option<Vec<u8>>,
    /// The cursor at which the query should stop.
    pub end_cursor: Option<Vec<u8>>,
    /// Property names to project on
    pub projection: Vec<Cow<'static, str>>,
    /// Properties to return distinct results on
    pub distinct_on: Vec<Cow<'static, str>>,
    pub order: Vec<PropertyOrder>,
    pub limit: i32,
    pub offset: i32,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            kind: "".into(),
            filter: None,
            start_cursor: None,
            end_cursor: None,
            projection: Vec::new(),
            distinct_on: Vec::new(),
            order: Vec::new(),
            limit: 1000,
            offset: 0,
        }
    }
}

impl Into<google_datastore1::api::Query> for Query {
    fn into(self) -> google_datastore1::api::Query {
        google_datastore1::api::Query {
            kind: if self.kind.is_empty() {
                    Some(Vec::new())
                } else {
                    let kind = google_datastore1::api::KindExpression {
                        name: Some(self.kind.into_owned())
                    };
                    Some(vec![kind])
                },
            filter: self.filter.map(Filter::into),
            start_cursor: self.start_cursor,
            end_cursor: self.end_cursor,
            projection: Some(self.projection.into_iter().map(|name| google_datastore1::api::Projection {
                property: Some(google_datastore1::api::PropertyReference { name: Some(name.into_owned() )}),
            }).collect()),
            distinct_on: Some(self.distinct_on.into_iter().map(|name| google_datastore1::api::PropertyReference {
                name: Some(name.into_owned()),
            }).collect()),
            order: Some(self.order.into_iter().map(PropertyOrder::into).collect()),
            limit: Some(self.limit),
            offset: Some(self.offset),
            ..Default::default()
        }
    }
}
