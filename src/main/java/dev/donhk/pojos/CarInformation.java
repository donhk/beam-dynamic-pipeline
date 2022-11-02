package dev.donhk.pojos;

import java.io.Serializable;
import java.time.LocalDateTime;

public class CarInformation implements Serializable {
    // id,car_model,car_make,city
    private final long id;
    private final String carModel;
    private final String carMake;
    private final String city;
    private final LocalDateTime time;

    public CarInformation(long id, String carModel, String carMake, String city, LocalDateTime time) {
        this.id = id;
        this.carModel = carModel;
        this.carMake = carMake;
        this.city = city;
        this.time = time;
    }

    public long getId() {
        return id;
    }

    public String getCarModel() {
        return carModel;
    }

    public String getCarMake() {
        return carMake;
    }

    public String getCity() {
        return city;
    }

    @Override
    public String toString() {
        return "CarInformation{" +
                "id=" + id +
                ", carModel='" + carModel + '\'' +
                ", carMake='" + carMake + '\'' +
                ", city='" + city + '\'' +
                ", time=" + time +
                '}';
    }

    public LocalDateTime getTime() {
        return time;
    }
}
