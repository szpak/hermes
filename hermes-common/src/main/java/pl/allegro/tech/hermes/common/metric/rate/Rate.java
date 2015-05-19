package pl.allegro.tech.hermes.common.metric.rate;

public enum Rate {
    MINUTES_1("m1_rate"),
    MINUTES_5("m5_rate"),
    MINUTES_15("m15_rate");

    private final String rate;

    Rate(String rate) {
        this.rate = rate;
    }

    @Override
    public String toString() {
        return rate;
    }
}
