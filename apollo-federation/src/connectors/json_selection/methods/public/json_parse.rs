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
/// $("{\"key\":\"value\"}")->jsonParse     would result in { "key": "value" }
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

    let JSON::String(s) = data else {
        return (
            None,
            vec![ApplyToError::new(
                format!(
                    "Method ->{} can only parse strings. Found: {}",
                    method_name.as_ref(),
                    data
                ),
                input_path.to_vec(),
                method_name.range(),
                spec,
            )],
        );
    };

    match serde_json::from_str::<serde_json::Value>(s.as_str()) {
        Ok(val) => {
            // Convert from serde_json::Value to serde_json_bytes::Value via .into()
            let bytes_val: JSON = val.into();
            (Some(bytes_val), Vec::new())
        }
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
    }
}
#[allow(dead_code)] // method type-checking disabled until we add name resolution
fn json_parse_shape(
    context: &ShapeContext,
    method_name: &WithRange<String>,
    _method_args: Option<&MethodArgs>,
    input_shape: Shape,
    _dollar_shape: Shape,
) -> Shape {
    // jsonParse requires string input
    if !(Shape::string([]).accepts(&input_shape) || input_shape.accepts(&Shape::unknown([]))) {
        return Shape::error_with_partial(
            format!(
                "Method ->{} can only parse strings. Found: {}",
                method_name.as_ref(),
                input_shape
            ),
            Shape::none(),
            method_name.shape_location(context.source_id()),
        );
    }

    // jsonParse can return any JSON value, so we return unknown
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
    #[case(json!("{\"key\":\"value\"}"), json!({ "key": "value" }), vec![])]
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
                    Some(3..14),
                    ConnectSpec::latest(),
                )],
            ),
        );
    }

    #[test]
    fn json_parse_should_error_for_invalid_json_string() {
        let result = selection!("$->jsonParse").apply_to(&json!("not valid json"));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse failed to parse JSON:")
        );
    }

    #[test]
    fn json_parse_should_error_for_non_string_input() {
        let result = selection!("$->jsonParse").apply_to(&json!(42));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse can only parse strings. Found: 42")
        );
    }

    #[test]
    fn json_parse_should_error_for_object_input() {
        let result = selection!("$->jsonParse").apply_to(&json!({"a": 1}));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse can only parse strings. Found:")
        );
    }

    #[test]
    fn json_parse_should_error_for_array_input() {
        let result = selection!("$->jsonParse").apply_to(&json!([1, 2, 3]));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse can only parse strings. Found:")
        );
    }

    #[test]
    fn json_parse_should_error_for_null_input() {
        let result = selection!("$->jsonParse").apply_to(&json!(null));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse can only parse strings. Found: null")
        );
    }

    #[test]
    fn json_parse_should_error_for_boolean_input() {
        let result = selection!("$->jsonParse").apply_to(&json!(true));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse can only parse strings. Found: true")
        );
    }

    #[test]
    fn json_parse_should_error_for_empty_string() {
        let result = selection!("$->jsonParse").apply_to(&json!(""));
        assert!(result.0.is_none());
        assert!(!result.1.is_empty());
        assert!(
            result.1[0]
                .message()
                .contains("Method ->jsonParse failed to parse JSON:")
        );
    }

    #[test]
    fn json_parse_roundtrip_with_json_stringify() {
        let original = json!({"key": "value", "num": 42, "arr": [1, 2, 3]});
        let (stringified, errors) = selection!("$->jsonStringify").apply_to(&original);
        assert!(errors.is_empty());
        let stringified = stringified.unwrap();

        let (parsed, errors) = selection!("$->jsonParse").apply_to(&stringified);
        assert!(errors.is_empty());
        assert_eq!(parsed, Some(original));
    }
}
