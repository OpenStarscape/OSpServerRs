use super::*;

/// [Orbital Elements on Wikipedia](https://en.wikipedia.org/wiki/Orbital_elements) may be helpful
/// in understanding this struct
pub struct OrbitData {
    /// Size of the semi-major axis (longest radius) (commonly a)
    semi_major: f64,
    /// Size of the semi-minor axis (shortest radius) (commonly b)
    semi_minor: f64,
    /// Angle (in radians) of the orbit compared to the global X/Y plane (commonly i)
    inclination: f64,
    /// Angle (in radians, on the global X/Y plane) of the ascending node (point where orbit crosses
    /// global X/Y plane going up) (commonly Ω (idk wtf that is either))
    ascending_node: f64,
    /// Angle (in radiuans, in the orbit space) of the periapsis (body's closest point to the
    /// parent) relative to the ascending node (commonly ω)
    periapsis: f64,
    /// Some time at which the body was/will be at the periapsis
    start_time: f64,
    /// Time it takes for a full orbit to complete. Calculatable from parent mass and G, but MUST
    /// be updated atomically with the rest of the orbit.
    period_time: f64,
    /// The "gravity parent" of the body. Should always be the same as the dedicated property of
    /// that name. Duplicated here because it MUST be updated atomically with the rest of the orbit
    /// parameters.
    parent: EntityKey,
}

impl From<OrbitData> for Value {
    fn from(orbit: OrbitData) -> Self {
        if orbit.parent.is_null() {
            Value::Null
        } else {
            let array: Vec<Value> = vec![
                orbit.semi_major.into(),
                orbit.semi_minor.into(),
                orbit.inclination.into(),
                orbit.ascending_node.into(),
                orbit.periapsis.into(),
                orbit.start_time.into(),
                orbit.period_time.into(),
                orbit.parent.into(),
            ];
            array.into()
        }
    }
}

/// A conduit that implements a body's orbit property
pub struct OrbitConduit {
    subscribers: SyncSubscriberList,
    body: EntityKey,
    cached_parent: Mutex<EntityKey>,
}

impl OrbitConduit {
    pub fn new(body: EntityKey) -> Self {
        Self {
            subscribers: SyncSubscriberList::new(),
            body,
            cached_parent: Mutex::new(EntityKey::null()),
        }
    }

    fn for_each_parent_subscribable<F: Fn(&dyn Subscribable)>(
        state: &State,
        parent: EntityKey,
        f: &F,
    ) -> RequestResult<()> {
        if !parent.is_null() {
            let parent_body = state.component::<Body>(parent)?;
            f(&parent_body.position);
            f(&parent_body.velocity);
            f(&parent_body.mass);
        }
        Ok(())
    }

    fn for_each_subscribable<F: Fn(&dyn Subscribable)>(
        &self,
        state: &State,
        f: &F,
    ) -> RequestResult<()> {
        let body = state.component::<Body>(self.body)?;
        Self::for_each_parent_subscribable(state, *self.cached_parent.lock().unwrap(), f)?;
        f(&body.gravity_parent);
        f(&body.position);
        f(&body.velocity);
        f(&body.mass);
        Ok(())
    }

    /// Ensures we are subscribed to the properties of the currently correct parent, and returns it
    fn update_parent(&self, state: &State) -> EntityKey {
        let parent = *state
            .component::<Body>(self.body)
            .expect("OrbitConduit body does not exist")
            .gravity_parent;
        let mut cached_parent = self.cached_parent.lock().unwrap();
        if parent != *cached_parent {
            let _ = Self::for_each_parent_subscribable(state, *cached_parent, &|s| {
                self.subscribers.unsubscribe_all(state, s);
            });
            *cached_parent = parent;
            let _ = Self::for_each_parent_subscribable(state, *cached_parent, &|s| {
                self.subscribers.subscribe_all(state, s);
            });
        }
        *cached_parent
    }
}

impl Conduit<Option<OrbitData>, ReadOnlyPropSetType> for OrbitConduit {
    fn output(&self, state: &State) -> RequestResult<Option<OrbitData>> {
        let parent = self.update_parent(state);
        let body = state.component::<Body>(self.body)?;
        if let Ok(parent_body) = state.component::<Body>(parent) {
            let gm = GRAVITATIONAL_CONSTANT * *parent_body.mass;
        } else {
        }
        /*
        Ok(OrbitData {
            semi_major: 100.0,
            semi_minor: 50.0,
            inclination: 1.0,
            ascending_node: 0.5,
            periapsis: 2.0,
            start_time: 0.0,
            period_time: 10.0,
            parent,
        })
        */
        Ok(Some(OrbitData {
            semi_major: 100.0,
            semi_minor: 50.0,
            inclination: TAU / 6.0,
            ascending_node: 0.0,
            periapsis: TAU / 3.0,
            start_time: 0.0,
            period_time: 10.0,
            parent,
        }))
    }

    fn input(&self, _: &mut State, _: ReadOnlyPropSetType) -> RequestResult<()> {
        unreachable!()
    }
}

impl Subscribable for OrbitConduit {
    fn subscribe(&self, state: &State, subscriber: &Arc<dyn Subscriber>) -> RequestResult<()> {
        // If the parent isn't initialized, we could miss notifications if we don't set it up here
        self.update_parent(state);
        self.for_each_subscribable(state, &|s| {
            s.subscribe(state, subscriber)
                .or_log_error("subscribing to OrbitConduit");
        })?;
        self.subscribers.add(subscriber)?;
        Ok(())
    }

    fn unsubscribe(&self, state: &State, subscriber: &Weak<dyn Subscriber>) -> RequestResult<()> {
        // No need to update parent here, it reflects the currently subscribed to things which is
        // all that matters.
        self.for_each_subscribable(state, &|s| {
            s.unsubscribe(state, subscriber)
                .or_log_error("unsubscribing from OrbitConduit");
        })?;
        self.subscribers.remove(subscriber)?;
        Ok(())
    }
}

// TODO: test
