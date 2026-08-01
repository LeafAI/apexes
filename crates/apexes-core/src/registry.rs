//! [`Registry`]: a process-wide (or test-local), thread-safe service
//! locator keyed by `(TypeId of T, business key of type K)`. Rust has
//! no runtime reflection, so lookup is by Rust type (`TypeId::of::<T>()`)
//! rather than by name -- `T` is the VALUE TYPE ITSELF (e.g. `Arc<dyn
//! ExecutionGateway>` when a trait object needs shared ownership; a
//! plain `u32` or any other `'static` type otherwise) -- `register`/
//! `get` do not additionally wrap `T` in another `Arc`.
//!
//! Two usage patterns:
//! - **"Exactly one instance, no business key needed"** (e.g. "the"
//!   `Arc<dyn ExecutionGateway>`): register/look up with `K = ()`, and
//!   prefer `one()`/`try_one()` over `get()` at the call site.
//! - **"Possibly multiple instances, keyed by some business identity"**
//!   (e.g. one `Arc<dyn MarketDataProvider>` per `Venue`): register/
//!   look up with `K = Venue` (or whatever key type is meaningful) via
//!   `get()`.
//!
//! See `docs/superpowers/specs/2026-08-01-apexes-ems-design.md` §二 for
//! full design rationale.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{OnceLock, RwLock};

/// Any type usable as a registry key: `Eq + Hash + Clone + Send + Sync +
/// 'static`. Blanket-implemented for every type satisfying these bounds,
/// including `()` (the "no key needed" case).
pub trait RegistryKey: Any + Eq + Hash + Clone + Send + Sync + 'static {}
impl<K: Any + Eq + Hash + Clone + Send + Sync + 'static> RegistryKey for K {}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("an instance of {type_name} is already registered under this key")]
    AlreadyRegistered { type_name: &'static str },
    #[error("no instance of {type_name} is registered")]
    NotRegistered { type_name: &'static str },
    #[error("expected exactly one instance of {type_name}, found {count}")]
    AmbiguousRegistration {
        type_name: &'static str,
        count: usize,
    },
}

/// One type's worth of registered instances, keyed by a type-erased key.
/// Stored as `Box<dyn Any + Send + Sync>` because different `T`s use
/// different concrete `K`s; the outer `Registry` downcasts back to the
/// caller's requested `(T, K)` combination on every access. `count`
/// tracks how many `(key, value)` pairs exist for this `T` across all
/// keys, independent of what `K` is, so `one()`/`try_one()` can answer
/// "how many total?" without needing to know `K` generically.
struct TypeEntry {
    map: Box<dyn Any + Send + Sync>,
    count: usize,
}

pub struct Registry {
    entries: RwLock<HashMap<TypeId, TypeEntry>>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Registers `value` under `key`. Fails with
    /// `Err(AlreadyRegistered)` if `(T, key)` already has an instance --
    /// no silent overwrite. Production code paths always use this
    /// method; `register_or_skip` (below) is test-only.
    pub fn register<T, K>(&self, key: K, value: T) -> Result<(), RegistryError>
    where
        T: Any + Send + Sync,
        K: RegistryKey,
    {
        let type_id = TypeId::of::<T>();
        let mut entries = self.entries.write().unwrap_or_else(|p| p.into_inner());

        let entry = entries.entry(type_id).or_insert_with(|| TypeEntry {
            map: Box::new(HashMap::<K, T>::new()),
            count: 0,
        });
        let map = entry
            .map
            .downcast_mut::<HashMap<K, T>>()
            .expect("Registry: type registered with inconsistent key type K for this T");

        if map.contains_key(&key) {
            return Err(RegistryError::AlreadyRegistered {
                type_name: std::any::type_name::<T>(),
            });
        }
        map.insert(key, value);
        entry.count += 1;
        Ok(())
    }

    /// Test-only convenience: silently no-ops instead of erroring if
    /// `(T, key)` is already registered. Used by test setup helpers
    /// (e.g. `fresh_manager`) that run under a shared global `Registry`
    /// where multiple parallel tests might otherwise race to register
    /// the same mock implementation. **Never call this from production
    /// code** -- production paths must use `register` so a genuine
    /// misconfiguration (double registration) is caught immediately.
    pub fn register_or_skip<T, K>(&self, key: K, value: T)
    where
        T: Any + Send + Sync,
        K: RegistryKey,
    {
        let _ = self.register(key, value);
    }

    /// Returns every instance of `T` currently registered, keyed by `K`.
    pub fn get<T, K>(&self) -> HashMap<K, T>
    where
        T: Any + Send + Sync + Clone,
        K: RegistryKey,
    {
        let type_id = TypeId::of::<T>();
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        match entries.get(&type_id) {
            Some(entry) => entry
                .map
                .downcast_ref::<HashMap<K, T>>()
                .expect("Registry: type registered with inconsistent key type K for this T")
                .clone(),
            None => HashMap::new(),
        }
    }

    /// Returns the sole registered value of `T` if exactly one exists
    /// (via `TypeEntry::count`, tracked independent of `K`); `Ok(None)`
    /// if zero exist; `Err(AmbiguousRegistration)` if 2+ exist. Shared
    /// implementation for both `one` and `try_one`.
    fn one_of_any_key<T>(&self) -> Result<Option<T>, RegistryError>
    where
        T: Any + Send + Sync + Clone,
    {
        let type_id = TypeId::of::<T>();
        let entries = self.entries.read().unwrap_or_else(|p| p.into_inner());
        let Some(entry) = entries.get(&type_id) else {
            return Ok(None);
        };
        if entry.count > 1 {
            return Err(RegistryError::AmbiguousRegistration {
                type_name: std::any::type_name::<T>(),
                count: entry.count,
            });
        }
        let map = entry.map.downcast_ref::<HashMap<(), T>>().expect(
            "Registry::one()/try_one() require T to always be registered with key K = () -- \
             use get::<T, K>() directly if T uses a non-unit key",
        );
        Ok(map.get(&()).cloned())
    }

    /// Asserts exactly one instance of `T` is registered (regardless of
    /// its key -- but see the module-level doc comment's `K = ()`
    /// constraint) and returns it. `Err` if 0 or 2+ exist.
    pub fn one<T>(&self) -> Result<T, RegistryError>
    where
        T: Any + Send + Sync + Clone,
    {
        self.one_of_any_key::<T>()?
            .ok_or(RegistryError::NotRegistered {
                type_name: std::any::type_name::<T>(),
            })
    }

    /// Like `one`, but 0 registered is `Ok(None)` instead of `Err` (2+
    /// is still `Err`). Used for genuinely-optional single components
    /// (e.g. `CompositeRiskChecker`: absent means "no risk checking
    /// configured", a valid and common state).
    pub fn try_one<T>(&self) -> Result<Option<T>, RegistryError>
    where
        T: Any + Send + Sync + Clone,
    {
        self.one_of_any_key::<T>()
    }
}

/// Process-wide singleton `Registry`. Production code (`main.rs`,
/// runner entry points) defaults to this; tests may use it too (this
/// project's convention, see design spec §三 "测试策略") or construct
/// an isolated `Registry::new()` when full test isolation is preferred.
pub fn global() -> &'static Registry {
    static GLOBAL: OnceLock<Registry> = OnceLock::new();
    GLOBAL.get_or_init(Registry::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    trait Greeter: Send + Sync {
        fn greet(&self) -> String;
    }

    struct EnglishGreeter;
    impl Greeter for EnglishGreeter {
        fn greet(&self) -> String {
            "hello".to_string()
        }
    }

    struct FrenchGreeter;
    impl Greeter for FrenchGreeter {
        fn greet(&self) -> String {
            "bonjour".to_string()
        }
    }

    #[test]
    fn register_and_get_single_instance_with_unit_key() {
        let registry = Registry::new();
        let greeter: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        registry
            .register::<Arc<dyn Greeter>, ()>((), greeter)
            .unwrap();

        let all = registry.get::<Arc<dyn Greeter>, ()>();
        assert_eq!(all.len(), 1);
        assert_eq!(all.get(&()).unwrap().greet(), "hello");
    }

    #[test]
    fn register_multiple_instances_under_distinct_keys() {
        let registry = Registry::new();
        let en: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        let fr: Arc<dyn Greeter> = Arc::new(FrenchGreeter);
        registry
            .register::<Arc<dyn Greeter>, &'static str>("en", en)
            .unwrap();
        registry
            .register::<Arc<dyn Greeter>, &'static str>("fr", fr)
            .unwrap();

        let all = registry.get::<Arc<dyn Greeter>, &'static str>();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("en").unwrap().greet(), "hello");
        assert_eq!(all.get("fr").unwrap().greet(), "bonjour");
    }

    #[test]
    fn register_fails_on_duplicate_key() {
        let registry = Registry::new();
        let en1: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        let en2: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        registry.register::<Arc<dyn Greeter>, ()>((), en1).unwrap();

        let result = registry.register::<Arc<dyn Greeter>, ()>((), en2);
        assert!(matches!(
            result,
            Err(RegistryError::AlreadyRegistered { .. })
        ));
    }

    #[test]
    fn get_returns_empty_map_when_nothing_registered() {
        let registry = Registry::new();
        let all = registry.get::<Arc<dyn Greeter>, ()>();
        assert!(all.is_empty());
    }

    #[test]
    fn register_or_skip_does_not_error_on_duplicate() {
        let registry = Registry::new();
        let en1: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        let en2: Arc<dyn Greeter> = Arc::new(FrenchGreeter);
        registry.register::<Arc<dyn Greeter>, ()>((), en1).unwrap();

        // Does not panic or return anything -- silently keeps the first.
        registry.register_or_skip::<Arc<dyn Greeter>, ()>((), en2);

        let all = registry.get::<Arc<dyn Greeter>, ()>();
        assert_eq!(all.len(), 1);
        assert_eq!(all.get(&()).unwrap().greet(), "hello"); // still the first one
    }

    #[test]
    fn different_types_do_not_collide_under_the_same_key() {
        let registry = Registry::new();
        let greeter: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        registry
            .register::<Arc<dyn Greeter>, ()>((), greeter)
            .unwrap();
        // A completely different T at the same () key must not collide.
        registry.register::<u32, ()>((), 42u32).unwrap();

        let greeters = registry.get::<Arc<dyn Greeter>, ()>();
        let numbers = registry.get::<u32, ()>();
        assert_eq!(greeters.len(), 1);
        assert_eq!(numbers.len(), 1);
        assert_eq!(*numbers.get(&()).unwrap(), 42);
    }

    #[test]
    fn one_returns_the_single_registered_instance() {
        let registry = Registry::new();
        let greeter: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        registry
            .register::<Arc<dyn Greeter>, ()>((), greeter)
            .unwrap();

        let result = registry.one::<Arc<dyn Greeter>>().unwrap();
        assert_eq!(result.greet(), "hello");
    }

    #[test]
    fn one_errors_when_nothing_registered() {
        let registry = Registry::new();
        let result = registry.one::<Arc<dyn Greeter>>();
        assert!(matches!(result, Err(RegistryError::NotRegistered { .. })));
    }

    #[test]
    fn one_errors_when_two_or_more_registered() {
        let registry = Registry::new();
        let en: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        let fr: Arc<dyn Greeter> = Arc::new(FrenchGreeter);
        registry
            .register::<Arc<dyn Greeter>, &'static str>("en", en)
            .unwrap();
        registry
            .register::<Arc<dyn Greeter>, &'static str>("fr", fr)
            .unwrap();

        let result = registry.one::<Arc<dyn Greeter>>();
        assert!(matches!(
            result,
            Err(RegistryError::AmbiguousRegistration { count: 2, .. })
        ));
    }

    #[test]
    fn try_one_returns_ok_none_when_nothing_registered() {
        let registry = Registry::new();
        let result = registry.try_one::<Arc<dyn Greeter>>().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn try_one_returns_the_single_instance_when_one_registered() {
        let registry = Registry::new();
        let greeter: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        registry
            .register::<Arc<dyn Greeter>, ()>((), greeter)
            .unwrap();

        let result = registry.try_one::<Arc<dyn Greeter>>().unwrap();
        assert_eq!(result.unwrap().greet(), "hello");
    }

    #[test]
    fn try_one_still_errors_when_two_or_more_registered() {
        let registry = Registry::new();
        let en: Arc<dyn Greeter> = Arc::new(EnglishGreeter);
        let fr: Arc<dyn Greeter> = Arc::new(FrenchGreeter);
        registry
            .register::<Arc<dyn Greeter>, &'static str>("en", en)
            .unwrap();
        registry
            .register::<Arc<dyn Greeter>, &'static str>("fr", fr)
            .unwrap();

        let result = registry.try_one::<Arc<dyn Greeter>>();
        assert!(matches!(
            result,
            Err(RegistryError::AmbiguousRegistration { count: 2, .. })
        ));
    }

    #[test]
    fn global_returns_the_same_instance_across_calls() {
        #[derive(Clone)]
        struct GlobalRegistryProbe;
        let a = global();
        let b = global();
        a.register_or_skip::<GlobalRegistryProbe, ()>((), GlobalRegistryProbe);
        assert_eq!(b.get::<GlobalRegistryProbe, ()>().len(), 1);
    }
}
