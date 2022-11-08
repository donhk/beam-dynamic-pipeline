package dev.donhk.pojos;

import java.io.Serializable;
import java.time.LocalDateTime;

public class CarInformation implements Serializable {
    // car_id,car_model,car_make,city,car_time,cost,promo
    private final long id;
    private final String carModel;
    private final String carMake;
    private final String city;
    private final LocalDateTime carTime;
    private final double cost;
    private final double promo;

    public CarInformation(long id, String carModel, String carMake, String city, LocalDateTime carTime, double cost, double promo) {
        this.id = id;
        this.carModel = carModel;
        this.carMake = carMake;
        this.city = city;
        this.carTime = carTime;
        this.cost = cost;
        this.promo = promo;
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

    public LocalDateTime getCarTime() {
        return carTime;
    }

    public double getCost() {
        return cost;
    }

    public double getPromo() {
        return promo;
    }

    @Override
    public String toString() {
        return "CarInformation{" +
                "id=" + id +
                ", carModel='" + carModel + '\'' +
                ", carMake='" + carMake + '\'' +
                ", city='" + city + '\'' +
                ", carTime=" + carTime +
                ", cost=" + cost +
                ", promo=" + promo +
                '}';
    }
}
