//! `SearchSpace`: maps each tunable parameter's name to its list of
//! candidate values, and expands the Cartesian product of all
//! combinations for `GridSearchOptimizer` (or is sent as-is, without
//! expansion, to an external process via `ExternalProcessOptimizer`).

use rust_decimal::Decimal;
use std::collections::HashMap;

/// Maps each tunable parameter's name to its list of candidate values.
#[derive(Debug, Clone, Default)]
pub struct SearchSpace {
    parameters: HashMap<String, Vec<Decimal>>,
}

impl SearchSpace {
    pub fn new() -> Self {
        Self {
            parameters: HashMap::new(),
        }
    }

    /// Adds a parameter and its candidate values. Calling this again
    /// with the same `name` replaces the previous candidate list
    /// (last-write-wins, matching `HashMap::insert`'s own semantics).
    pub fn with_parameter(mut self, name: impl Into<String>, values: Vec<Decimal>) -> Self {
        self.parameters.insert(name.into(), values);
        self
    }

    /// Returns a reference to the underlying parameter-name ->
    /// candidate-values map, e.g. for `ExternalProcessOptimizer` to
    /// serialize the raw search space (not the expanded combinations)
    /// to an external process.
    pub fn parameters(&self) -> &HashMap<String, Vec<Decimal>> {
        &self.parameters
    }

    /// Returns every combination as a `HashMap<String, Decimal>`, one
    /// per grid point. Returns an empty `Vec` if there are no
    /// parameters, or if any single parameter's candidate list is empty
    /// (standard Cartesian-product semantics: any empty dimension makes
    /// the whole product empty) -- "no parameters to search" and "an
    /// empty search space" are both treated as "zero combinations to
    /// evaluate", never as "one combination with fewer keys".
    pub fn combinations(&self) -> Vec<HashMap<String, Decimal>> {
        if self.parameters.is_empty() || self.parameters.values().any(|values| values.is_empty()) {
            return Vec::new();
        }

        let mut names: Vec<&String> = self.parameters.keys().collect();
        names.sort();

        let mut combinations: Vec<HashMap<String, Decimal>> = vec![HashMap::new()];
        for name in names {
            let values = &self.parameters[name];
            let mut next = Vec::with_capacity(combinations.len() * values.len());
            for existing in &combinations {
                for value in values {
                    let mut combo = existing.clone();
                    combo.insert(name.clone(), *value);
                    next.push(combo);
                }
            }
            combinations = next;
        }
        combinations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn single_parameter_produces_one_combination_per_value() {
        let space = SearchSpace::new().with_parameter("x", vec![dec!(1), dec!(2), dec!(3)]);
        let combos = space.combinations();
        assert_eq!(combos.len(), 3);
        let mut values: Vec<Decimal> = combos.iter().map(|c| c["x"]).collect();
        values.sort();
        assert_eq!(values, vec![dec!(1), dec!(2), dec!(3)]);
    }

    #[test]
    fn two_parameters_produce_cartesian_product() {
        let space = SearchSpace::new()
            .with_parameter("x", vec![dec!(1), dec!(2)])
            .with_parameter("y", vec![dec!(10), dec!(20), dec!(30)]);
        let combos = space.combinations();
        // 2 * 3 = 6 combinations.
        assert_eq!(combos.len(), 6);
        for combo in &combos {
            assert!(combo.contains_key("x"));
            assert!(combo.contains_key("y"));
        }
        // Every (x, y) pair should be present exactly once.
        let mut pairs: Vec<(Decimal, Decimal)> = combos.iter().map(|c| (c["x"], c["y"])).collect();
        pairs.sort();
        let mut expected = vec![
            (dec!(1), dec!(10)),
            (dec!(1), dec!(20)),
            (dec!(1), dec!(30)),
            (dec!(2), dec!(10)),
            (dec!(2), dec!(20)),
            (dec!(2), dec!(30)),
        ];
        expected.sort();
        assert_eq!(pairs, expected);
    }

    #[test]
    fn empty_search_space_produces_no_combinations() {
        let space = SearchSpace::new();
        assert!(space.combinations().is_empty());
    }

    #[test]
    fn parameter_with_empty_candidate_list_produces_no_combinations() {
        let space = SearchSpace::new()
            .with_parameter("x", vec![dec!(1), dec!(2)])
            .with_parameter("y", vec![]);
        assert!(space.combinations().is_empty());
    }

    #[test]
    fn with_parameter_called_twice_for_same_name_replaces_candidates() {
        let space = SearchSpace::new()
            .with_parameter("x", vec![dec!(1), dec!(2)])
            .with_parameter("x", vec![dec!(99)]);
        let combos = space.combinations();
        assert_eq!(combos.len(), 1);
        assert_eq!(combos[0]["x"], dec!(99));
    }
}
