import re
import pandas as pd
from urllib.parse import urlparse

def validate_column(data, rules):
    for index, value in data.items():
        if pd.isna(value):
            # Skip validation for NaN values or implement special handling if required
            continue

        if isinstance(value, str):
            issues = validate_string(value, rules)
        elif isinstance(value, (int, float)):
            issues = validate_numerical(value, rules)
        elif isinstance(value, list):
            issues = validate_array(value, rules)
        elif isinstance(value, dict):
            issues = validate_object(value, rules)
        else:
            continue

        if (len(issues) > 0):
            return issues

    return []

# String validation
def validate_string(value, rules):
    issues = []
    if 'minLength' in rules and len(value) < rules['minLength']:
        issues.append(f"String is shorter than the minimum length of {rules['minLength']}.")
    if 'maxLength' in rules and len(value) > rules['maxLength']:
        issues.append(f"String is longer than the maximum length of {rules['maxLength']}.")
    if 'pattern' in rules and not re.match(rules['pattern'], value):
        issues.append(f"String does not match the required pattern: {rules['pattern']}.")
    if 'enum' in rules and value not in rules['enum']:
        issues.append(f"String is not one of the permitted values in the enumeration.")
    if 'format' in rules:
        # todo: pre-compile regexes
        if rules['format'] == 'email' and not re.match(r"[^@]+@[^@]+\.[^@]+", value):
            issues.append("String is not a valid email.")
        if rules['format'] == 'uri' and not urlparse(value).scheme:
            issues.append("String is not a valid URI.")
        if rules['format'] == 'uuid' and not re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z", value):
            issues.append("String is not a valid UUID.")
    return issues

# Numerical validation
def validate_numerical(value, rules):
    issues = []
    if 'minimum' in rules and value < rules['minimum']:
        issues.append(f"Value is less than the minimum allowed value of {rules['minimum']}.")
    if 'maximum' in rules and value > rules['maximum']:
        issues.append(f"Value is greater than the maximum allowed value of {rules['maximum']}.")
    if 'exclusiveMinimum' in rules and value <= rules['exclusiveMinimum']:
        issues.append(f"Value is less than or equal to the exclusive minimum value of {rules['exclusiveMinimum']}.")
    if 'exclusiveMaximum' in rules and value >= rules['exclusiveMaximum']:
        issues.append(f"Value is greater than or equal to the exclusive maximum value of {rules['exclusiveMaximum']}.")
    if 'enum' in rules and value not in rules['enum']:
        issues.append("Value is not one of the permitted values in the enumeration.")
    return issues

# Array validation
def validate_array(values, rules):
    issues = []
    if 'minItems' in rules and len(values) < rules['minItems']:
        issues.append(f"Array has fewer items than the minimum of {rules['minItems']}.")
    if 'maxItems' in rules and len(values) > rules['maxItems']:
        issues.append(f"Array has more items than the maximum of {rules['maxItems']}.")
    if 'uniqueItems' in rules and rules['uniqueItems'] and len(values) != len(set(values)):
        issues.append("Array items are not unique.")
    # todo: Further validation for 'items' if necessary
    return issues

# Object validation
def validate_object(value, rules):
    issues = []
    for key, val in rules['properties'].items():
        if key not in value:
            issues.append(f"Key '{key}' is missing from the object.")
        # todo: Further validation based on the type of property
    return issues
