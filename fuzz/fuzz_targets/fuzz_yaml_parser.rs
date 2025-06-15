#![no_main]
use libfuzzer_sys::fuzz_target;
use yamlbase::yaml::parse_yaml_string;

fuzz_target!(|data: &[u8]| {
    // Only fuzz valid UTF-8 strings
    if let Ok(yaml_str) = std::str::from_utf8(data) {
        // Try to parse the YAML
        // We don't care about the result, just that it doesn't panic
        let _ = parse_yaml_string(yaml_str);
    }
});