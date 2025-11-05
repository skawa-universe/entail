use super::*;

use std::borrow::Cow;
use strum::{Display, EnumString};

/// Represents a paginated result set from a query.
///
/// This struct holds a collection of items (T) retrieved in the current request
/// and a cursor used to fetch the subsequent page of items.
#[derive(Debug)]
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

    /// Consumes the `QueryResult<T>` and transforms its items into
    /// a `QueryResult<U>` using the provided closure.
    pub fn map<U, F>(self, f: F) -> QueryResult<U>
    where
        F: FnMut(T) -> U,
    {
        // 1. Destructure the original QueryResult.
        let QueryResult { items, end_cursor } = self;

        // 2. Map the items vector using the closure.
        let transformed_items = items
            .into_iter() // Consuming iterator
            .map(f)
            .collect();

        // 3. Construct and return the new QueryResult<U>.
        QueryResult {
            items: transformed_items,
            end_cursor, // The cursor is simply moved/copied.
        }
    }
}

impl<'a, T> QueryResult<T>
where
    T: 'a, // T must live at least as long as 'a
{
    /// Transforms a reference to the `QueryResult<T>` into a
    /// `QueryResult<U>` using the provided closure.
    /// This does *not* consume the original QueryResult.
    pub fn map_ref<U, F>(&'a self, mut f: F) -> QueryResult<U>
    where
        F: FnMut(&'a T) -> U,
    {
        // 1. Iterate over references to the items.
        let transformed_items = self.items
            .iter()
            .map(|item_ref| f(item_ref))
            .collect();

        // 2. Clone the end_cursor since the original is kept.
        QueryResult {
            items: transformed_items,
            end_cursor: self.end_cursor.clone(),
        }
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

/// Represents a filter used in a Datastore query.
///
/// Filters are used to constrain the results returned by a query,
/// much like a `WHERE` clause in SQL.
#[derive(Clone, Debug)]
pub enum Filter {
    /// A composite filter that combines multiple sub-filters using a logical operator.
    ///
    /// Currently, only the `And` operator is supported.
    Composite(CompositeFilterOperator, Vec<Filter>),
    /// Represents a filter based on a property's value.
    ///
    /// This is the most common type of filter, used for comparisons like `property > value`.
    /// It consists of three components:
    /// 1.  **Property Name**: The name of the property to filter on. While this is typically
    ///     a static string, it is represented as a `Cow<'static, str>` to allow for
    ///     both borrowed and owned strings.
    /// 2.  **Operator**: The [`FilterOperator`] to use for the comparison (e.g., `Equal`, `GreaterThan`).
    /// 3.  **Value**: The `Value` to compare the property against.
    Property(Cow<'static, str>, FilterOperator, Value),
}

impl Filter {
    /// Combines multiple filters with a logical `AND` operator.
    ///
    /// This is a convenience method for creating a `Composite` filter. It handles
    /// edge cases by returning `None` for an empty vector or unwrapping a single
    /// filter from a vector of one.
    ///
    /// ## Parameters
    /// - `filters`: A `Vec` of `Filter`s to be combined.
    ///
    /// ## Returns
    /// An `Option<Filter>` containing the combined filter, or `None` if the input vector
    /// was empty.
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

impl FilterOperator {
    /// Creates a new `Filter::Property` variant using this operator.
    ///
    /// This is a convenience method for constructing a filter that compares a
    /// specific property against a given value.
    ///
    /// ## Parameters
    /// - `property_name`: The name of the property to filter on.
    /// - `value`: The value to compare the property against.
    ///
    /// ## Returns
    /// A completed [`Filter::Property`] ready to be used in a query.
    pub fn of(self, property_name: impl Into<Cow<'static, str>>, value: Value) -> Filter {
        Filter::Property(property_name.into(), self, value)
    }
}

/// The logical operator used to combine sub-filters in a `Composite` filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CompositeFilterOperator {
    /// The logical `AND` operator. All sub-filters must evaluate to true for the composite
    /// filter to be true.
    And,
}

/// The comparison operator used in a `Property` filter.
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

/// The direction in which to order query results.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderDirection {
    /// Ascending order (A-Z, 0-9). This is the default.
    ASCENDING,
    /// Descending order (Z-A, 9-0).
    DESCENDING,
}

/// A single property by which to order the results of a query.
///
/// An `PropertyOrder` consists of a property's name and the desired sort direction.
/// Multiple `PropertyOrder`s can be used to define a multi-level sort.
#[derive(Clone, Debug)]
pub struct PropertyOrder {
    /// The name of the property to order by.
    pub name: Cow<'static, str>,
    /// The direction of the sort, either `ASCENDING` or `DESCENDING`.
    pub direction: OrderDirection,
}

impl PropertyOrder {
    /// Creates a new `PropertyOrder` instance.
    ///
    /// ## Parameters
    /// - `name`: The name of the property to order by.
    /// - `direction`: The direction of the sort.
    pub fn new(name: impl Into<Cow<'static, str>>, direction: OrderDirection) -> Self {
        Self { name: name.into(), direction }
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

/// Represents a query to be executed against the Datastore.
///
/// A `Query` object defines the criteria for retrieving entities, including the
/// kind of entity, filters, sorting, and pagination options.
#[derive(Clone, Debug)]
pub struct Query {
    /// The **kind** of entity to query.
    ///
    /// Use an empty string to perform a kindless query, which can return entities
    /// of any kind. The Datastore API supports querying at most one kind at a time.
    pub kind: Cow<'static, str>,
    /// An optional **filter** to apply to the entities.
    ///
    /// This allows you to restrict the query results based on property values,
    /// similar to a `WHERE` clause in SQL.
    pub filter: Option<Filter>,
    /// An optional **start cursor** for pagination.
    ///
    /// If provided, the query will begin returning results from this cursor's position,
    /// which is useful for fetching the next page of a large result set.
    pub start_cursor: Option<Vec<u8>>,
    /// An optional **end cursor** for pagination.
    ///
    /// The query will stop returning results at this cursor's position. This can be
    /// used to limit the results to a specific range.
    pub end_cursor: Option<Vec<u8>>,
    /// A list of property names to **project** on.
    ///
    /// This is a **projection query**, which returns only the specified properties,
    /// rather than the entire entity. This can reduce latency and cost.
    ///
    /// **Important:** Projection queries only work for properties that are
    /// included in an index. The special `__key__` property is always projectable.
    pub projection: Vec<Cow<'static, str>>,
    /// A list of property names to return **distinct** results on.
    ///
    /// This ensures that only entities with unique combinations of values for the
    /// specified properties are returned.
    ///
    /// **Important:** Like projections, `distinct_on` requires the specified
    /// properties to be part of a proper index.
    pub distinct_on: Vec<Cow<'static, str>>,
    /// A list of **property orders** to sort the results by.
    ///
    /// The results will be ordered according to the specified properties and their
    /// sort directions (ascending or descending).
    pub order: Vec<PropertyOrder>,
    /// The maximum number of results to return.
    ///
    /// A value of `0` means no limit. You never-ever want to run an unlimited query.
    pub limit: i32,
    /// The number of results to skip from the beginning of the result set.
    ///
    /// **Caveat**: While supported, using an offset can be inefficient and costly. The skipped
    /// entities are still read internally by Datastore, affecting query latency and
    /// billing. It is highly recommended to use `start_cursor` for pagination instead.
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
