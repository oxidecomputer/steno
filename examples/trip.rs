//!
//! Example from the canonical distributed sagas talk: suppose you have existing
//! functions to book a hotel, book a flight, book a car reservation, and charge
//! a payment card.  You also have functions to cancel a hotel, flight, or car
//! reservation and refund a credit card charge.  You want to implement a "book
//! trip" function whose implementation makes sure that you ultimately wind up
//! with all of these bookings (and having paid for it) or none (and having not
//! paid for it).
//!

// Names are given here for clarity, even when they're not needed.
#![allow(unused_variables)]

use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use steno::ActionContext;
use steno::ActionError;
use steno::ActionFunc;
use steno::NullSink;
use steno::SagaExecutor;
use steno::SagaId;
use steno::SagaTemplate;
use steno::SagaTemplateBuilder;
use steno::SagaType;
use uuid::Uuid;

//
// This is where we're going: this program will collect payment and book a whole
// trip that includes a hotel, flight, and car.  This will either all succeed or
// any steps that ran will be undone.  In a real example, you'd persist the saga
// log and use the saga recovery interface to resume execution after a crash.
//
#[tokio::main]
async fn main() {
    let trip_context = Arc::new(TripContext {});
    let params = TripParams {
        hotel_name: String::from("Springfield Palace Hotel"),
        flight_info: String::from("any flight"),
        car_info: String::from("1998 Canyonero"),
        charge_details: String::from("Moneybank Charge Card"),
    };
    book_trip(trip_context, params).await;
}

// Create a new "book trip" saga with the given parameters and then execute it.
async fn book_trip(trip_context: Arc<TripContext>, params: TripParams) {
    //
    // Build a saga template.  The template describes the actions that are part
    // of the saga (including the functions to be invoked to do each of the
    // steps) and how they depend on each other.  You can create this once and
    // cache it to book many trips.
    //
    let saga_template = Arc::new(make_trip_saga());

    //
    // Get ready to execute the saga.
    //

    // Each execution needs a new unique id.
    let saga_id = SagaId(Uuid::new_v4());

    //
    // You can provide a name to this instance of this program.  This will wind
    // up in the saga log so that if you have multiple instances, you can tell
    // which instances did what.
    //
    let creator = "myself";

    // Create an executor to execute the saga.
    let saga_exec = SagaExecutor::new(
        &saga_id,
        saga_template,
        creator,
        Arc::new(trip_context),
        params,
        Arc::new(NullSink),
    )
    .expect("failed to serialize saga parameters");

    //
    // Run the saga to completion.  This could take a while, depending on what
    // the saga does!  This traverses the DAG of actions, executing each one.
    // If one fails, then it's all unwound: any actions that previously
    // completed will be undone.
    //
    saga_exec.run().await;

    // Print the results.
    match saga_exec.result().kind {
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
            println!("action failed: {}", error.error_node_name);
            println!("error: {}", error.error_source);
        }
    }
}

/// Builds the saga template to book a trip.  A template describes the actions
/// that are part of the saga (including the functions to be invoked to do each
/// of the steps) and how they depend on each other.
fn make_trip_saga() -> SagaTemplate<TripSaga> {
    let mut builder = SagaTemplateBuilder::new();

    //
    // Somewhat arbitrarily, we're choosing to charge the credit card first,
    // then make all the bookings in parallel.  We could do these all in
    // parallel, or all sequentially, and the saga would still be correct, since
    // Steno guarantees that eventually either all actions will succeed or all
    // executed actions will be undone.
    //
    builder.append(
        // name of this action's output (can be used in subsequent actions)
        "payment",
        // human-readable label for the action
        "ChargeCreditCard",
        ActionFunc::new_action(
            // action function
            saga_charge_card,
            // undo function
            saga_refund_card,
        ),
    );

    builder.append_parallel(vec![
        (
            "hotel",
            "BookHotel",
            ActionFunc::new_action(saga_book_hotel, saga_cancel_hotel),
        ),
        (
            "flight",
            "BookFlight",
            ActionFunc::new_action(saga_book_flight, saga_cancel_flight),
        ),
        (
            "car",
            "BookCar",
            ActionFunc::new_action(saga_book_car, saga_cancel_car),
        ),
    ]);

    builder.build()
}

//
// Implementation of the trip saga
//

//
// Saga parameters.  Each trip will have a separate set of parameters.  We use
// plain strings here, but these can be any serializable / deserializable types.
//
#[derive(Debug, Deserialize, Serialize)]
struct TripParams {
    hotel_name: String,
    flight_info: String,
    car_info: String,
    charge_details: String,
}

//
// Application-specific context that we want to provide to every action in the
// saga.  This can be any object we want.  We'll pass it to Steno when the saga
// begins execution.  Steno passes it back to us in each action.  This makes it
// easy for us to access application-specific state, like a logger, HTTP
// clients, etc.
//
struct TripContext;

//
// Steno uses several type parameters that you specify by impl'ing the SagaType
// trait.
//
struct TripSaga;
impl SagaType for TripSaga {
    // Type for the saga's parameters
    type SagaParamsType = TripParams;

    // Type for the application-specific context (see above)
    type ExecContextType = Arc<TripContext>;
}

//
// Data types emitted by various saga actions.  These must be serializable and
// deserializable.  This is the only supported way to share data between
// actions in the same saga.
//

#[derive(Debug, Deserialize, Serialize)]
struct HotelReservation(String);
#[derive(Debug, Deserialize, Serialize)]
struct FlightReservation(String);
#[derive(Debug, Deserialize, Serialize)]
struct CarReservation(String);
#[derive(Debug, Deserialize, Serialize)]
struct PaymentConfirmation(String);

//
// Saga action implementations
//

async fn saga_charge_card(
    action_context: ActionContext<TripSaga>,
) -> Result<PaymentConfirmation, ActionError> {
    let trip_context = action_context.user_data();
    let charge_details = &action_context.saga_params().charge_details;
    // ... (make request to another service)
    Ok(PaymentConfirmation(String::from("123")))
}

async fn saga_refund_card(
    action_context: ActionContext<TripSaga>,
) -> Result<(), anyhow::Error> {
    //
    // Fetch the payment confirmation.  The undo function is only ever invoked
    // after the action function has succeeded.  This node is called "payment",
    // so we fetch our own action's output by looking up the data for "payment".
    //
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
    let hotel_name = &action_context.saga_params().hotel_name;
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
    let flight_info = &action_context.saga_params().flight_info;
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
    let car_info = &action_context.saga_params().car_info;
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
