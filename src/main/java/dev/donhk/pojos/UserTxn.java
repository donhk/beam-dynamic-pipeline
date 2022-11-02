package dev.donhk.pojos;

import java.io.Serializable;
import java.time.LocalDateTime;

public class UserTxn implements Serializable {
    // id,first_name,last_name,email,gender,time,amount
    private final long id;
    private final String firstName;
    private final String secondName;
    private final String email;
    private final String gender;
    private final LocalDateTime time;
    private final double amount;

    public UserTxn(long id,
                   String firstName,
                   String secondName,
                   String email,
                   String gender,
                   LocalDateTime time,
                   double amount) {
        this.id = id;
        this.firstName = firstName;
        this.secondName = secondName;
        this.email = email;
        this.gender = gender;
        this.time = time;
        this.amount = amount;
    }

    public long getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getSecondName() {
        return secondName;
    }

    public String getEmail() {
        return email;
    }

    public String getGender() {
        return gender;
    }

    public LocalDateTime getTime() {
        return time;
    }

    public double getAmount() {
        return amount;
    }
}
