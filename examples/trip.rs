//! Example from the canonical distributed sagas talk: suppose you have existing
//! functions to book a hotel, book a flight, book a car reservation, and charge
//! a payment card.  You also have functions to cancel a hotel, flight, or car
//! reservation and refund a credit card charge.  You want to implement a "book
//! trip" function whose implementation makes sure that you ultimately wind up
//! with all of these bookings (and having paid for it) or none (and having not
//! paid for it).

// Names are given here for clarity, even when they're not needed.
#![allow(unused_variables)]

use serde::Deserialize;
use serde::Serialize;
use slog::Drain;
use std::sync::Arc;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::ActionName;
use steno::ActionRegistry;
use steno::Dag;
use steno::DagBuilder;
use steno::SagaId;
use steno::SagaName;
use steno::SagaType;
use steno::SecClient;
use steno::UserNode;
use uuid::Uuid;

// This is where we're going: this program will collect payment and book a whole
// trip that includes a hotel, flight, and car.  This will either all succeed or
// any steps that ran will be undone.  In a real example, you'd persist the saga
// log and use the saga recovery interface to resume execution after a crash.
#[tokio::main]
async fn main() {
    let log = {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog::LevelFilter(drain, slog::Level::Warning).fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    };
    let sec = steno::sec(
        log.new(slog::o!()),
        Arc::new(steno::InMemorySecStore::new()),
    );
    let trip_context = Arc::new(TripContext {});
    let params = TripParams {
        hotel_name: String::from("Springfield Palace Hotel"),
        flight_info: String::from("any flight"),
        car_info: String::from("1998 Canyonero"),
        charge_details: String::from("Moneybank Charge Card"),
    };
    book_trip(sec, trip_context, params).await;
}

// Create a new "book trip" saga with the given parameters and then execute it.
async fn book_trip(
    sec: SecClient,
    trip_context: Arc<TripContext>,
    params: TripParams,
) {
    // Register the actions to run during saga execution.
    // This is created once for all sagas.
    let registry = make_trip_action_registry();

    // Build a saga DAG.  The DAG describes the actions that are part
    // of the saga (including the functions to be invoked to do each of the
    // steps) and how they depend on each other.  This is dynamically created
    // for each trip.
    let dag = make_trip_saga(params);

    // Get ready to execute the saga.

    // Each execution needs a new unique id.
    let saga_id = SagaId(Uuid::new_v4());

    // Create the saga.
    let saga_future = sec
        .saga_create(saga_id, Arc::new(trip_context), dag, registry)
        .await
        .expect("failed to create saga");

    // Set it running.
    sec.saga_start(saga_id).await.expect("failed to start saga running");

    // Wait for the saga to finish running.  This could take a while, depending
    // on what the saga does!  This traverses the DAG of actions, executing each
    // one.  If one fails, then it's all unwound: any actions that previously
    // completed will be undone.
    //
    // Note that the SEC will run all this regardless of whether you wait for it
    // here.  This is just a handle for you to know when the saga has finished.
    let result = saga_future.await;

    // Print the results.
    match result.kind {
        Ok(success) => {
            println!(
                "hotel:   {:?}",
                success.lookup_output::<HotelReservation>("hotel")
            );
            println!(
                "flight:  {:?}",
                success.lookup_output::<FlightReservation>("flight")
            );
            println!(
                "car:     {:?}",
                success.lookup_output::<CarReservation>("car")
            );
            println!(
                "payment: {:?}",
                success.lookup_output::<PaymentConfirmation>("payment")
            );
        }
        Err(error) => {
            println!("action failed: {}", error.error_node_name.as_ref());
            println!("error: {}", error.error_source);
        }
    }
}

/// Registers all actions for the saga
fn make_trip_action_registry() -> Arc<ActionRegistry<TripSaga>> {
    let mut registry = ActionRegistry::new();

    registry.register(ActionFunc::new_action(
        // action name
        "payment",
        // action function
        saga_charge_card,
        // undo function
        saga_refund_card,
    ));

    registry.register(ActionFunc::new_action(
        "hotel",
        saga_book_hotel,
        saga_cancel_hotel,
    ));

    registry.register(ActionFunc::new_action(
        "flight",
        saga_book_flight,
        saga_cancel_flight,
    ));

    registry.register(ActionFunc::new_action(
        "car",
        saga_book_car,
        saga_cancel_car,
    ));

    Arc::new(registry)
}

/// Builds the saga DAG to book a trip. The builder methods describes the actions
/// that are part of the saga (including the functions to be invoked to do each
/// of the steps) and how they depend on each other.
fn make_trip_saga(params: TripParams) -> Arc<Dag> {
    let name = SagaName::new("book-trip");
    let mut builder = DagBuilder::new(name, params);

    // Somewhat arbitrarily, we're choosing to charge the credit card first,
    // then make all the bookings in parallel.  We could do these all in
    // parallel, or all sequentially, and the saga would still be correct, since
    // Steno guarantees that eventually either all actions will succeed or all
    // executed actions will be undone.
    builder.append(UserNode::action(
        // name of this action's output (can be used in subsequent actions)
        "payment",
        // human-readable label for the action
        "ChargeCreditCard",
        // The name of the action to run. This is the lookup key in the
        // ActionRegistry.
        ActionName::new("payment"),
    ));

    builder.append_parallel(vec![
        UserNode::action("hotel", "BookHotel", ActionName::new("hotel")),
        UserNode::action("flight", "BookFlight", ActionName::new("flight")),
        UserNode::action("car", "BookCar", ActionName::new("car")),
    ]);

    Arc::new(builder.build())
}

// Implementation of the trip saga

// Saga parameters.  Each trip will have a separate set of parameters.  We use
// plain strings here, but these can be any serializable / deserializable types.
#[derive(Debug, Deserialize, Serialize)]
struct TripParams {
    hotel_name: String,
    flight_info: String,
    car_info: String,
    charge_details: String,
}

// Application-specific context that we want to provide to every action in the
// saga.  This can be any object we want.  We'll pass it to Steno when the saga
// begins execution.  Steno passes it back to us in each action.  This makes it
// easy for us to access application-specific state, like a logger, HTTP
// clients, etc.
#[derive(Debug)]
struct TripContext;

// Steno uses several type parameters that you specify by impl'ing the SagaType
// trait.
#[derive(Debug)]
struct TripSaga;
impl SagaType for TripSaga {
    // Type for the application-specific context (see above)
    type ExecContextType = Arc<TripContext>;
}

// Data types emitted by various saga actions.  These must be serializable and
// deserializable.  This is the only supported way to share data between
// actions in the same saga.

#[derive(Debug, Deserialize, Serialize)]
struct HotelReservation(String);
#[derive(Debug, Deserialize, Serialize)]
struct FlightReservation(String);
#[derive(Debug, Deserialize, Serialize)]
struct CarReservation(String);
#[derive(Debug, Deserialize, Serialize)]
struct PaymentConfirmation(String);

// Saga action implementations

async fn saga_charge_card(
    action_context: ActionContext<TripSaga>,
) -> Result<PaymentConfirmation, ActionError> {
    let trip_context = action_context.user_data();
    let params = action_context.saga_params::<TripParams>()?;
    let charge_details = &params.charge_details;
    // ... (make request to another service)
    Ok(PaymentConfirmation(String::from("123")))
}

async fn saga_refund_card(
    action_context: ActionContext<TripSaga>,
) -> Result<(), anyhow::Error> {
    // Fetch the payment confirmation.  The undo function is only ever invoked
    // after the action function has succeeded.  This node is called "payment",
    // so we fetch our own action's output by looking up the data for "payment".
    let trip_context = action_context.user_data();
    let p: PaymentConfirmation = action_context.lookup("payment")?;
    // ... (make request to another service -- must not fail)
    Ok(())
}

async fn saga_book_hotel(
    action_context: ActionContext<TripSaga>,
) -> Result<HotelReservation, ActionError> {
    /* ... */
    let trip_context = action_context.user_data();
    let params = action_context.saga_params::<TripParams>()?;
    let hotel_name = &params.hotel_name;
    // ... (make request to another service)
    Ok(HotelReservation(String::from("123")))
}

async fn saga_cancel_hotel(
    action_context: ActionContext<TripSaga>,
) -> Result<(), anyhow::Error> {
    /* ... */
    let trip_context = action_context.user_data();
    let confirmation: HotelReservation = action_context.lookup("hotel")?;
    // ... (make request to another service -- must not fail)
    Ok(())
}

async fn saga_book_flight(
    action_context: ActionContext<TripSaga>,
) -> Result<FlightReservation, ActionError> {
    /* ... */
    let trip_context = action_context.user_data();
    let params = action_context.saga_params::<TripParams>()?;
    let flight_info = &params.flight_info;
    // ... (make request to another service)
    Ok(FlightReservation(String::from("123")))
}

async fn saga_cancel_flight(
    action_context: ActionContext<TripSaga>,
) -> Result<(), anyhow::Error> {
    /* ... */
    let trip_context = action_context.user_data();
    let confirmation: FlightReservation = action_context.lookup("flight")?;
    // ... (make request to another service -- must not fail)
    Ok(())
}

async fn saga_book_car(
    action_context: ActionContext<TripSaga>,
) -> Result<CarReservation, ActionError> {
    /* ... */
    let trip_context = action_context.user_data();
    let params = action_context.saga_params::<TripParams>()?;
    let car_info = &params.car_info;
    // ... (make request to another service)
    Ok(CarReservation(String::from("123")))
}

async fn saga_cancel_car(
    action_context: ActionContext<TripSaga>,
) -> Result<(), anyhow::Error> {
    /* ... */
    let trip_context = action_context.user_data();
    let confirmation: CarReservation = action_context.lookup("car")?;
    // ... (make request to another service -- must not fail)
    Ok(())
}
