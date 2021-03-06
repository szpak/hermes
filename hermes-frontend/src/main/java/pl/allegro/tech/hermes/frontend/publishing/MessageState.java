package pl.allegro.tech.hermes.frontend.publishing;

public class MessageState {

    public enum State {
        WAITING_FOR_FIRST_PORTION_OF_DATA,
        PARSING,
        PARSED,
        SENDING_TO_KAFKA_PRODUCER_QUEUE,
        SENDING_TO_KAFKA
    }

    private volatile State state = State.WAITING_FOR_FIRST_PORTION_OF_DATA;

    public synchronized void setState(State state) {
        this.state = state;
    }

    public synchronized State getState() {
        return state;
    }
}
