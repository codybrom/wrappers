//! OpenAPI 3.0+ specification parsing
//!
//! This module provides types and functions for parsing OpenAPI specifications
//! and extracting endpoint/schema information for FDW table generation.

use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Represents an OpenAPI 3.0+ specification
#[derive(Debug, Deserialize)]
pub struct OpenApiSpec {
    pub openapi: String,
    pub info: Info,
    #[serde(default)]
    pub servers: Vec<Server>,
    #[serde(default)]
    pub paths: HashMap<String, PathItem>,
    #[serde(default)]
    pub components: Option<Components>,
}

#[derive(Debug, Deserialize)]
pub struct Info {
    pub title: String,
    #[serde(default)]
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct Server {
    pub url: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PathItem {
    #[serde(default)]
    pub get: Option<Operation>,
    #[serde(default)]
    pub post: Option<Operation>,
    #[serde(default)]
    pub put: Option<Operation>,
    #[serde(default)]
    pub patch: Option<Operation>,
    #[serde(default)]
    pub delete: Option<Operation>,
    #[serde(default)]
    pub parameters: Vec<Parameter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Operation {
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parameters: Vec<Parameter>,
    #[serde(default)]
    pub request_body: Option<RequestBody>,
    #[serde(default)]
    pub responses: HashMap<String, Response>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Parameter {
    pub name: String,
    #[serde(rename = "in")]
    pub location: String, // "query", "path", "header", "cookie"
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub schema: Option<Schema>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RequestBody {
    #[serde(default)]
    pub content: HashMap<String, MediaType>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Deserialize)]
pub struct Response {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub content: HashMap<String, MediaType>,
}

#[derive(Debug, Deserialize)]
pub struct MediaType {
    #[serde(default)]
    pub schema: Option<Schema>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    #[serde(default)]
    pub schema_type: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, Schema>,
    #[serde(default)]
    pub items: Option<Box<Schema>>,
    #[serde(rename = "$ref")]
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub nullable: bool,
    #[serde(rename = "allOf")]
    #[serde(default)]
    pub all_of: Vec<Schema>,
    #[serde(rename = "oneOf")]
    #[serde(default)]
    pub one_of: Vec<Schema>,
    #[serde(rename = "anyOf")]
    #[serde(default)]
    pub any_of: Vec<Schema>,
}

#[derive(Debug, Deserialize)]
pub struct Components {
    #[serde(default)]
    pub schemas: HashMap<String, Schema>,
    #[serde(default)]
    pub security_schemes: HashMap<String, SecurityScheme>,
}

#[derive(Debug, Deserialize)]
pub struct SecurityScheme {
    #[serde(rename = "type")]
    pub scheme_type: String,
    #[serde(default)]
    pub scheme: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "in")]
    #[serde(default)]
    pub location: Option<String>,
    #[serde(rename = "bearerFormat")]
    #[serde(default)]
    pub bearer_format: Option<String>,
}

impl OpenApiSpec {
    /// Parse an OpenAPI spec from a JSON value
    pub fn from_json(json: &JsonValue) -> Result<Self, String> {
        serde_json::from_value(json.clone()).map_err(|e| format!("Failed to parse OpenAPI spec: {}", e))
    }

    /// Parse an OpenAPI spec from a JSON string
    pub fn from_str(s: &str) -> Result<Self, String> {
        serde_json::from_str(s).map_err(|e| format!("Failed to parse OpenAPI spec: {}", e))
    }

    /// Get the base URL from the spec (first server URL)
    pub fn base_url(&self) -> Option<&str> {
        self.servers.first().map(|s| s.url.as_str())
    }

    /// Get all endpoint paths that support GET operations (for querying)
    pub fn get_endpoints(&self) -> Vec<EndpointInfo> {
        let mut endpoints = Vec::new();

        for (path, path_item) in &self.paths {
            // Skip paths with path parameters (like /users/{id}) for list operations
            // These are handled via pushdown
            if path.contains('{') {
                continue;
            }

            if let Some(ref op) = path_item.get {
                let response_schema = self.get_response_schema(op);
                endpoints.push(EndpointInfo {
                    path: path.clone(),
                    operation_id: op.operation_id.clone(),
                    summary: op.summary.clone().or_else(|| op.description.clone()),
                    response_schema,
                    supports_post: path_item.post.is_some(),
                    supports_patch: path_item.patch.is_some(),
                    supports_delete: path_item.delete.is_some(),
                    parameters: Self::merge_parameters(&path_item.parameters, &op.parameters),
                });
            }
        }

        endpoints.sort_by(|a, b| a.path.cmp(&b.path));
        endpoints
    }

    /// Merge path-level and operation-level parameters
    fn merge_parameters(path_params: &[Parameter], op_params: &[Parameter]) -> Vec<Parameter> {
        let mut params: Vec<Parameter> = path_params.iter().cloned().collect();
        for op_param in op_params {
            if !params.iter().any(|p| p.name == op_param.name && p.location == op_param.location) {
                params.push(op_param.clone());
            }
        }
        params
    }

    /// Get the response schema for a successful response (200, 201, or default)
    fn get_response_schema(&self, op: &Operation) -> Option<Schema> {
        let response = op
            .responses
            .get("200")
            .or_else(|| op.responses.get("201"))
            .or_else(|| op.responses.get("default"))?;

        let media_type = response
            .content
            .get("application/json")
            .or_else(|| response.content.values().next())?;

        media_type.schema.clone()
    }

    /// Resolve a $ref to its schema
    pub fn resolve_ref(&self, reference: &str) -> Option<&Schema> {
        // Handle refs like "#/components/schemas/User"
        let parts: Vec<&str> = reference.trim_start_matches("#/").split('/').collect();
        if parts.len() == 3 && parts[0] == "components" && parts[1] == "schemas" {
            self.components.as_ref()?.schemas.get(parts[2])
        } else {
            None
        }
    }

    /// Recursively resolve a schema, following $ref pointers and handling composition
    pub fn resolve_schema(&self, schema: &Schema) -> Schema {
        // First resolve any $ref
        if let Some(ref reference) = schema.reference {
            if let Some(resolved) = self.resolve_ref(reference) {
                return self.resolve_schema(resolved);
            }
        }

        // Handle allOf by merging all properties (intersection - all schemas apply)
        if !schema.all_of.is_empty() {
            return self.merge_schemas(&schema.all_of, false);
        }

        // Handle oneOf by merging all possible properties as nullable (union - one of the schemas)
        if !schema.one_of.is_empty() {
            return self.merge_schemas(&schema.one_of, true);
        }

        // Handle anyOf by merging all possible properties as nullable (union - any of the schemas)
        if !schema.any_of.is_empty() {
            return self.merge_schemas(&schema.any_of, true);
        }

        schema.clone()
    }

    /// Merge multiple schemas into one
    /// If `make_nullable` is true, all properties become optional (for oneOf/anyOf)
    fn merge_schemas(&self, schemas: &[Schema], make_nullable: bool) -> Schema {
        let mut merged = Schema {
            schema_type: Some("object".to_string()),
            properties: HashMap::new(),
            required: Vec::new(),
            ..Default::default()
        };

        for sub_schema in schemas {
            let resolved = self.resolve_schema(sub_schema);

            // Merge properties
            for (name, mut prop_schema) in resolved.properties {
                if make_nullable {
                    prop_schema.nullable = true;
                }
                // If property already exists, keep the existing one (first wins)
                merged.properties.entry(name).or_insert(prop_schema);
            }

            // For allOf, all required fields stay required
            // For oneOf/anyOf, nothing is required since we don't know which variant
            if !make_nullable {
                merged.required.extend(resolved.required);
            }
        }

        // Deduplicate required fields
        merged.required.sort();
        merged.required.dedup();

        merged
    }
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            schema_type: None,
            format: None,
            properties: HashMap::new(),
            items: None,
            reference: None,
            required: Vec::new(),
            nullable: false,
            all_of: Vec::new(),
            one_of: Vec::new(),
            any_of: Vec::new(),
        }
    }
}

impl Clone for Parameter {
    fn clone(&self) -> Self {
        Parameter {
            name: self.name.clone(),
            location: self.location.clone(),
            required: self.required,
            schema: self.schema.clone(),
            description: self.description.clone(),
        }
    }
}

/// Extracted endpoint information for table generation
#[derive(Debug)]
pub struct EndpointInfo {
    pub path: String,
    pub operation_id: Option<String>,
    pub summary: Option<String>,
    pub response_schema: Option<Schema>,
    pub supports_post: bool,
    pub supports_patch: bool,
    pub supports_delete: bool,
    pub parameters: Vec<Parameter>,
}

impl EndpointInfo {
    /// Generate a table name from the endpoint path
    pub fn table_name(&self) -> String {
        // Convert /users -> users, /api/v1/customers -> customers
        let name = self
            .path
            .trim_start_matches('/')
            .split('/')
            .last()
            .unwrap_or("unknown");

        // Convert kebab-case to snake_case
        name.replace('-', "_")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_spec() {
        let spec_json = r#"{
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0"},
            "paths": {}
        }"#;

        let spec = OpenApiSpec::from_str(spec_json).unwrap();
        assert_eq!(spec.openapi, "3.0.0");
        assert_eq!(spec.info.title, "Test API");
    }

    #[test]
    fn test_endpoint_table_name() {
        let endpoint = EndpointInfo {
            path: "/api/v1/user-accounts".to_string(),
            operation_id: None,
            summary: None,
            response_schema: None,
            supports_post: false,
            supports_patch: false,
            supports_delete: false,
            parameters: vec![],
        };

        assert_eq!(endpoint.table_name(), "user_accounts");
    }

    #[test]
    fn test_resolve_ref() {
        let spec_json = r#"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "User": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"}
                        },
                        "required": ["id"]
                    }
                }
            }
        }"#;

        let spec = OpenApiSpec::from_str(spec_json).unwrap();
        let user_schema = spec.resolve_ref("#/components/schemas/User").unwrap();

        assert_eq!(user_schema.schema_type, Some("object".to_string()));
        assert!(user_schema.properties.contains_key("id"));
        assert!(user_schema.properties.contains_key("name"));
        assert!(user_schema.required.contains(&"id".to_string()));
    }

    #[test]
    fn test_allof_merges_properties() {
        let spec_json = r##"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Base": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"}
                        },
                        "required": ["id"]
                    },
                    "Extended": {
                        "allOf": [
                            {"$ref": "#/components/schemas/Base"},
                            {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "email": {"type": "string"}
                                },
                                "required": ["name"]
                            }
                        ]
                    }
                }
            }
        }"##;

        let spec = OpenApiSpec::from_str(spec_json).unwrap();
        let extended = spec.resolve_ref("#/components/schemas/Extended").unwrap();
        let resolved = spec.resolve_schema(extended);

        // Should have all properties from both schemas
        assert!(resolved.properties.contains_key("id"));
        assert!(resolved.properties.contains_key("name"));
        assert!(resolved.properties.contains_key("email"));

        // Required from both should be merged
        assert!(resolved.required.contains(&"id".to_string()));
        assert!(resolved.required.contains(&"name".to_string()));
    }

    #[test]
    fn test_oneof_merges_as_nullable() {
        let spec_json = r#"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Response": {
                        "oneOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "user_id": {"type": "string"},
                                    "user_name": {"type": "string"}
                                },
                                "required": ["user_id"]
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "org_id": {"type": "string"},
                                    "org_name": {"type": "string"}
                                },
                                "required": ["org_id"]
                            }
                        ]
                    }
                }
            }
        }"#;

        let spec = OpenApiSpec::from_str(spec_json).unwrap();
        let response = spec.resolve_ref("#/components/schemas/Response").unwrap();
        let resolved = spec.resolve_schema(response);

        // Should have properties from all variants
        assert!(resolved.properties.contains_key("user_id"));
        assert!(resolved.properties.contains_key("user_name"));
        assert!(resolved.properties.contains_key("org_id"));
        assert!(resolved.properties.contains_key("org_name"));

        // All properties should be nullable (since we don't know which variant)
        assert!(resolved.properties.get("user_id").unwrap().nullable);
        assert!(resolved.properties.get("org_id").unwrap().nullable);

        // Nothing should be required for oneOf
        assert!(resolved.required.is_empty());
    }

    #[test]
    fn test_anyof_merges_as_nullable() {
        let spec_json = r#"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Flexible": {
                        "anyOf": [
                            {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"}
                                }
                            },
                            {
                                "type": "object",
                                "properties": {
                                    "title": {"type": "string"}
                                }
                            }
                        ]
                    }
                }
            }
        }"#;

        let spec = OpenApiSpec::from_str(spec_json).unwrap();
        let flexible = spec.resolve_ref("#/components/schemas/Flexible").unwrap();
        let resolved = spec.resolve_schema(flexible);

        // Should have properties from all variants
        assert!(resolved.properties.contains_key("name"));
        assert!(resolved.properties.contains_key("title"));

        // All should be nullable
        assert!(resolved.properties.get("name").unwrap().nullable);
        assert!(resolved.properties.get("title").unwrap().nullable);
    }

    #[test]
    fn test_nested_ref_resolution() {
        let spec_json = r##"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Address": {
                        "type": "object",
                        "properties": {
                            "street": {"type": "string"},
                            "city": {"type": "string"}
                        }
                    },
                    "Person": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "address": {"$ref": "#/components/schemas/Address"}
                        }
                    }
                }
            }
        }"##;

        let spec = OpenApiSpec::from_str(spec_json).unwrap();
        let person = spec.resolve_ref("#/components/schemas/Person").unwrap();
        let resolved = spec.resolve_schema(person);

        assert!(resolved.properties.contains_key("name"));
        assert!(resolved.properties.contains_key("address"));

        // The address property should still have a $ref (we resolve at property level when needed)
        let address_prop = resolved.properties.get("address").unwrap();
        assert!(address_prop.reference.is_some() || !address_prop.properties.is_empty());
    }
}
