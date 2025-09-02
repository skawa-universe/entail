use darling::{FromField, FromDeriveInput};
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, GenericArgument, PathArguments, Type, Ident};
use syn::spanned::Spanned;
use convert_case::{Case, Casing};

/// Checks if the given `syn::Path` represents a `String` type.
/// This function handles both simple "String" and fully qualified "std::string::String".
///
/// # Arguments
///
/// * `path` - A reference to a `syn::Path` to check.
///
/// # Returns
///
/// `true` if the path represents a `String` type, `false` otherwise.
fn is_string_type(path: &syn::Path) -> bool {
    let segments = &path.segments;
    if let Some(last_segment) = segments.last() {
        if last_segment.ident != "String" {
            return false;
        }

        // Handle simple "String"
        if segments.len() == 1 {
            return true;
        }

        // Handle "std::string::String"
        if segments.len() == 3
            && segments[0].ident == "std"
            && segments[1].ident == "string"
        {
            return true;
        }
    }
    false
}

/// Checks if the given `syn::Path` represents a specific custom type.
///
/// This function works by comparing the segments of the `syn::Path` with the
/// segments of the target type in reverse order. It will return `true` if
/// the end of the `syn::Path` matches the entire sequence of `type_segments`.
///
/// For example, if `type_segments` is `["entail", "ds", "Key"]`:
/// - `entail::ds::Key` will match.
/// - `ds::Key` will match.
/// - `Key` will match.
/// - `ds::Another` will not match.
/// - `std::string::String` will not match.
///
/// # Arguments
///
/// * `path` - A reference to the `syn::Path` to check.
/// * `type_segments` - A slice of string slices (`&[&str]`) representing the
///   segments of the target type, e.g., `&["entail", "ds", "Key"]`.
///
/// # Returns
///
/// `true` if the path is a suffix match for the given type, `false` otherwise.
fn is_custom_type(path: &syn::Path, type_segments: &[&str]) -> bool {
    // If the type segments are empty, there's nothing to match.
    if type_segments.is_empty() {
        return false;
    }

    // Compare segments from the end, backwards, to find a suffix match.
    path.segments.iter().rev()
        .zip(type_segments.iter().rev())
        .all(|(path_segment, type_str)| {
            path_segment.ident.to_string() == *type_str
        })
}

const KEY_TYPE_PATH: &[&str] = &["entail", "ds", "Key"];

fn is_key_type(path: &syn::Path) -> bool {
    is_custom_type(path, KEY_TYPE_PATH)
}

/// Checks if the given `syn::Path` represents a `Cow<'static, str>` type.
/// This function handles both simple "Cow" and fully qualified "std::borrow::Cow",
/// and specifically verifies the generic arguments for `'static` lifetime and `str` type.
///
/// # Arguments
///
/// * `path` - A reference to a `syn::Path` to check.
///
/// # Returns
///
/// `true` if the path represents a `Cow<'static, str>` type, `false` otherwise.
fn is_cow_static_str_type(path: &syn::Path) -> bool {
    let segments = &path.segments;

    if let Some(last_segment) = segments.last() {
        // 1. Check if the last segment's identifier is "Cow"
        if last_segment.ident != "Cow" {
            return false;
        }

        // 2. Handle simple "Cow" or fully qualified "std::borrow::Cow"
        let is_base_cow_path = match segments.len() {
            1 => true, // Simple "Cow"
            3 => { // "std::borrow::Cow"
                segments[0].ident == "std" && segments[1].ident == "borrow"
            },
            _ => false, // Any other length is not a valid Cow path we're looking for
        };

        if !is_base_cow_path {
            return false;
        }

        // 3. Check generic arguments: we expect `<'static, str>`
        if let PathArguments::AngleBracketed(args) = &last_segment.arguments {
            // Ensure there are exactly two generic arguments
            if args.args.len() != 2 {
                return false;
            }

            // Check the first argument: should be a 'static lifetime
            let is_static_lifetime = if let Some(GenericArgument::Lifetime(lifetime)) = args.args.first() {
                lifetime.ident == "static"
            } else {
                false
            };

            if !is_static_lifetime {
                return false;
            }

            // Check the second argument: should be the `str` type
            let is_str_type = if let Some(GenericArgument::Type(Type::Path(type_path))) = args.args.last() {
                // For `str`, it's usually just `str` as a single segment
                type_path.path.segments.len() == 1 && type_path.path.segments[0].ident == "str"
            } else {
                false
            };

            return is_str_type; // Both lifetime and type must match
        }
    }
    false
}

/// Checks if the given `syn::Path` represents an `Option` type.
/// This function handles both simple "Option" and fully qualified "std::option::Option".
///
/// # Arguments
///
/// * `path` - A reference to a `syn::Path` to check.
///
/// # Returns
///
/// `true` if the path represents an `Option` type, `false` otherwise.
fn is_option_type(path: &syn::Path) -> bool {
    let segments = &path.segments;
    if let Some(last_segment) = segments.last() {
        if last_segment.ident != "Option" {
            return false;
        }

        // Handle simple "Option"
        if segments.len() == 1 {
            return true;
        }

        // Handle "std::option::Option"
        if segments.len() == 3
            && segments[0].ident == "std"
            && segments[1].ident == "option"
        {
            return true;
        }
    }
    false
}

/// Checks if the given `syn::Path` represents a `Vec` type.
/// This function handles both simple "Vec" and fully qualified "std::vec::Vec".
///
/// # Arguments
///
/// * `path` - A reference to a `syn::Path` to check.
///
/// # Returns
///
/// `true` if the path represents a `Vec` type, `false` otherwise.
fn is_vec_type(path: &syn::Path) -> bool {
    let segments = &path.segments;
    if let Some(last_segment) = segments.last() {
        // Check if the last segment's identifier is "Vec"
        if last_segment.ident != "Vec" {
            return false;
        }

        // Handle the simple "Vec" case (e.g., `Vec<u8>`)
        if segments.len() == 1 {
            return true;
        }

        // Handle the fully qualified "std::vec::Vec" case (e.g., `std::vec::Vec<i32>`)
        if segments.len() == 3
            && segments[0].ident == "std"
            && segments[1].ident == "vec"
        {
            return true;
        }
    }
    false
}

fn is_integer_type(path: &syn::Path) -> bool {
    path.is_ident("i32") || path.is_ident("u32") || path.is_ident("i64")
}

// Represents the parsed #[entail(...)] attribute for a field
#[derive(Debug, Default, FromField)]
#[darling(attributes(entail))]
struct EntailFieldAttribute {
    /// #[entail(key)] - Marks this field as the primary key
    #[darling(default)]
    pub key: bool,
    /// #[entail(field)] - Forces it to be a regular field (overrides 'key' inference)
    #[darling(default)]
    pub field: bool,
    /// #[entail(name = "custom_name")] - Overrides the Datastore property name
    #[darling(default)]
    pub name: Option<String>,
    /// #[entail(indexed)] - Ensures the field is always indexed
    #[darling(default)]
    pub indexed: bool,
    /// #[entail(unindexed)] - Prevents the field from being indexed
    #[darling(default)]
    pub unindexed: bool,
    /// #[entail(unindexed_nulls)] - Indexes Option<T> only if not None
    #[darling(default)]
    pub unindexed_nulls: bool,
}

// Represents the parsed #[entail(...)] attribute for the container (struct)
#[derive(Debug, Default, FromDeriveInput)]
#[darling(attributes(entail))]
struct EntailContainerAttribute {
    /// #[entail(rename_all = "camelCase")] - Global renaming policy
    #[darling(default)]
    pub rename_all: Option<String>,
    /// #[entail(name = "KindName")] - Overrides the Datastore Kind name
    #[darling(default)]
    pub name: Option<String>,
}

#[derive(Debug)]
struct ParsedField<'a> {
    name: &'a proc_macro2::Ident,
    ty_path: &'a syn::Path,
    attrs: EntailFieldAttribute,
    property_name: String,
}

impl<'a> ParsedField<'a> {
    fn build(f: &'a syn::Field, c: &'a EntailContainerAttribute) -> Option<Self> {
        // Check if the field is named
        let name = f.ident.as_ref()?;

        // Check if the type is a Path and extract it
        let ty_path = if let syn::Type::Path(ty_path) = &f.ty {
            &ty_path.path
        } else {
            return None;
        };

        let attrs = match EntailFieldAttribute::from_field(f) {
            Ok(attrs) => attrs,
            Err(e) => {
                e.write_errors();
                return None;
            }
        };

        let property_name = if let Some(s) = &attrs.name {
            s.clone()
        } else if c.rename_all.is_none() || c.rename_all.as_ref().unwrap() == "camelCase" {
            name.to_string().to_case(Case::Camel)
        } else {
            name.to_string()
        };

        Some(ParsedField { name, ty_path, attrs, property_name })
    }

    fn is_nullable(&self) -> bool { is_option_type(&self.ty_path) }

    fn is_array(&self) -> bool { is_vec_type(&self.ty_path) }

    fn type_path(&self) -> &'a syn::Path {
        if !self.is_nullable() && !self.is_array() {
            &self.ty_path
        } else {
            let last_segment = &self.ty_path.segments.last().unwrap();
            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                let ty = &args.args.first().unwrap();
                if let syn::GenericArgument::Type(syn::Type::Path(embedded_ty)) = ty {
                    &embedded_ty.path
                } else {
                    panic!("Unrecognized argument in {:?}", &last_segment.span())
                }
            } else {
                panic!("Unrecognized argument in {:?}", &last_segment.span())
            }
        }
    }

    fn create_property_name_lit(&self) -> syn::LitStr {
        syn::LitStr::new(&self.property_name, self.name.span())
    }
}

#[proc_macro_derive(Entail, attributes(entail))]
pub fn derive_entail(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let entail_input = match EntailContainerAttribute::from_derive_input(&input) {
        Ok(input) => input,
        Err(e) => return e.write_errors().into(),
    };

    let name = &input.ident;
    let raw_name = name.to_string();
    let kind_str = syn::LitStr::new(if let Some(custom_name) = &entail_input.name {
            custom_name
        } else {
            &raw_name
        }, name.span());
    let fields = match &input.data {
        syn::Data::Struct(syn::DataStruct { fields: syn::Fields::Named(fields), .. }) => &fields.named,
        _ => panic!("Entail can only be derived for structs with named fields"),
    };

    let parsed_fields: Vec<ParsedField> = fields.iter()
        .filter_map(|f| { ParsedField::build(&f, &entail_input) })
        .collect();
    let key_field: &ParsedField = parsed_fields.iter()
        .filter(|pf| pf.attrs.key || !pf.attrs.field && pf.name.to_string() == "key")
        .try_fold(None, |acc, item| match &acc {
            None => Ok(Some(item)),
            Some(_) => Err(()),
        }).ok()
        .unwrap_or_else(|| panic!("Multiple primary keys found on {:?}", &name.span()))
        .unwrap_or_else(|| panic!("No primary key found on {:?}", &name.span()));

    if key_field.is_array() {
        panic!("Keys cannot be arrays: {:?}", &key_field.name.span());
    }

    let key_field_name: &Ident = key_field.name;
    let key_initializer: proc_macro2::TokenStream = if is_cow_static_str_type(key_field.ty_path) || is_string_type(key_field.ty_path) {
        if key_field.is_nullable() {
            quote! {
                match &self.#key_field_name {
                    None => entail::ds::Key::new(#kind_str),
                    Some(name) => entail::ds::Key::new(#kind_str).with_name(name.clone()),
                }
            }
        } else {
            quote! {
                entail::ds::Key::new(#kind_str).with_name(self.#key_field_name.clone())
            }
        }
    } else if key_field.ty_path.is_ident("i64") {
        if key_field.is_nullable() {
            quote! {
                match &self.#key_field_name {
                    None => entail::ds::Key::new(#kind_str),
                    Some(id) => entail::ds::Key::new(#kind_str).with_id(id),
                }
            }
        } else {
            quote! {
                entail::ds::Key::new(#kind_str).with_id(&self.#key_field_name)
            }
        }
    } else if is_key_type(key_field.ty_path) {
        if key_field.is_nullable() {
            quote! {
                match &self.#key_field_name {
                    None => entail::ds::Key::new(#kind_str),
                    Some(key) => key,
                }
            }
        } else {
            quote! {
                self.#key_field_name
            }
        }
    } else {
        panic!("Invalid key type at {:?}", &key_field.ty_path.span());
    };

    let set_properties: Vec<proc_macro2::TokenStream> = parsed_fields.iter().map(|f| {
        if std::ptr::eq(key_field, f) {
            // the key is handled separately
            return quote! { };
        }
        let name: &proc_macro2::Ident = f.name;
        let property_name_lit: syn::LitStr = f.create_property_name_lit();
        let nullable: bool = f.is_nullable();
        let array: bool = f.is_array();
        let path: &syn::Path = f.type_path();

        let setter = if !f.attrs.unindexed && !f.attrs.unindexed_nulls || f.attrs.indexed {
            quote! { set_indexed }
        } else {
            quote! { set_unindexed }
        };

        macro_rules! gen_setter {
                ($ds_value:ident, $conversion:tt) => {
                    if nullable {
                        quote! {
                            e.#setter(#property_name_lit, match &self.#name {
                                Some(val) => entail::ds::Value::$ds_value($conversion),
                                None => entail::ds::Value::null(),
                            });
                        }
                    } else if array {
                        quote! {
                            e.#setter(#property_name_lit, entail::ds::Value::array(self.#name.iter()
                                .map(|val| entail::ds::Value::$ds_value($conversion))
                                .collect()));
                        }
                    } else {
                        quote! {{ let val = &self.#name; e.#setter(#property_name_lit, entail::ds::Value::$ds_value($conversion)); }}
                    }
                }
        }

        // blob is not implemented yet
        
        if is_string_type(path) {
            gen_setter!(unicode_string, (val.clone()))
        } else if is_cow_static_str_type(path) {
            gen_setter!(unicode_string, val)
        } else if is_integer_type(path) {
            gen_setter!(integer, (*val as i64))
        } else if path.is_ident("f32") || path.is_ident("f64") {
            gen_setter!(floating_point, (*val as f64))
        } else if path.is_ident("bool") {
            gen_setter!(boolean, val)
        } else if is_key_type(path) {
            gen_setter!(key, (val.clone()))
        } else {
            quote! { }
        }
    }).collect();
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let generated = quote! {
        impl #impl_generics entail::EntityModel for #name #type_generics #where_clause {
            fn to_ds_entity(&self) -> Result<entail::ds::Entity, entail::EntailError> {
                let mut e = entail::ds::Entity::new(#key_initializer);
                #(#set_properties)*
                Ok(e)
            }
        }
    };

    generated.into()
}
