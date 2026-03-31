use serde_json_bytes::Value as JSON;
use shape::Shape;

use crate::connectors::json_selection::ApplyToError;
use crate::connectors::json_selection::MethodArgs;
use crate::connectors::json_selection::ShapeContext;
use crate::connectors::json_selection::VarsWithPathsMap;
use crate::connectors::json_selection::immutable::InputPath;
use crate::connectors::json_selection::location::Ranged;
use crate::connectors::json_selection::location::WithRange;
use crate::connectors::spec::ConnectSpec;
use crate::impl_arrow_method;

impl_arrow_method!(JsonParseMethod, json_parse_method, json_parse_shape);
/// Parses a JSON string into a JSON value
/// The simplest possible example:
///
///
/// $->echo("{\"key\":\"value\"}")->jsonParse     would result in { "key": "value" }
fn json_parse_method(
    method_name: &WithRange<String>,
    method_args: Option<&MethodArgs>,
    data: &JSON,
    _vars: &VarsWithPathsMap,
    input_path: &InputPath<JSON>,
    spec: ConnectSpec,
) -> (Option<JSON>, Vec<ApplyToError>) {
    if method_args.is_some() {
        return (
            None,
            vec![ApplyToError::new(
                format!(
                    "Method ->{} does not take any arguments",
                    method_name.as_ref()
                ),
                input_path.to_vec(),
                method_name.range(),
                spec,
            )],
        );
    }

    match data {
        JSON::String(s) => match serde_json::from_str::<JSON>(s.as_str()) {
            Ok(val) => (Some(val), Vec::new()),
            Err(err) => (
                None,
                vec![ApplyToError::new(
                    format!(
                        "Method ->{} failed to parse JSON: {}",
                        method_name.as_ref(),
                        err
                    ),
                    input_path.to_vec(),
                    method_name.range(),
                    spec,
                )],
            ),
        },
        _ => (
            None,
            vec![ApplyToError::new(
                format!(
                    "Method ->{} requires a string input",
                    method_name.as_ref()
                ),
                input_path.to_vec(),
                method_name.range(),
                spec,
            )],
        ),
    }
}
#[allow(dead_code)] // method type-checking disabled until we add name resolution
fn json_parse_shape(
    context: &ShapeContext,
    method_name: &WithRange<String>,
    _method_args: Option<&MethodArgs>,
    _input_shape: Shape,
    _dollar_shape: Shape,
) -> Shape {
    Shape::unknown(method_name.shape_location(context.source_id()))
}

#[cfg(test)]
mod tests {
    use serde_json_bytes::json;

    use super::*;
    use crate::connectors::ApplyToError;
    use crate::selection;

    #[rstest::rstest]
    #[case(json!("null"), json!(null), vec![])]
    #[case(json!("true"), json!(true), vec![])]
    #[case(json!("false"), json!(false), vec![])]
    #[case(json!("42"), json!(42), vec![])]
    #[case(json!("10.8"), json!(10.8), vec![])]
    #[case(json!("\"hello world\""), json!("hello world"), vec![])]
    #[case(json!("[1,2,3]"), json!([1, 2, 3]), vec![])]
    #[case(json!("{\"key\":\"value\"}"), json!({"key": "value"}), vec![])]
    #[case(json!("[1,\"two\",true,null]"), json!([1, "two", true, null]), vec![])]
    fn json_parse_should_parse_various_structures(
        #[case] input: JSON,
        #[case] expected: JSON,
        #[case] errors: Vec<ApplyToError>,
    ) {
        assert_eq!(
            selection!("$->jsonParse").apply_to(&input),
            (Some(expected), errors),
        );
    }

    #[test]
    fn json_parse_should_error_when_provided_argument() {
        assert_eq!(
            selection!("$->jsonParse(1)").apply_to(&json!("null")),
            (
                None,
                vec![ApplyToError::new(
                    "Method ->jsonParse does not take any arguments".to_string(),
                    vec![json!("->jsonParse")],
                    Some(3..12),
                    ConnectSpec::latest(),
                )],
            ),
        );
    }

    #[test]
    fn json_parse_should_error_on_non_string_input() {
        assert_eq!(
            selection!("$->jsonParse").apply_to(&json!(42)),
            (
                None,
                vec![ApplyToError::new(
                    "Method ->jsonParse requires a string input".to_string(),
                    vec![json!("->jsonParse")],
                    Some(3..12),
                    ConnectSpec::latest(),
                )],
            ),
        );
    }

    #[test]
    fn json_parse_should_error_on_invalid_json_string() {
        let (result, errors) = selection!("$->jsonParse").apply_to(&json!("{invalid}"));
        assert_eq!(result, None);
        assert_eq!(errors.len(), 1);
        assert!(errors[0]
            .message()
            .starts_with("Method ->jsonParse failed to parse JSON:"));
    }
}
