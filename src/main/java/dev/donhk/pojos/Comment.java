package dev.donhk.pojos;

import java.io.Serializable;

public class Comment implements Serializable {
    // id,username,comment,background
    private final long id;
    private final String username;
    private final String comment;
    private final String background;

    public Comment(long id, String username, String comment, String background) {
        this.id = id;
        this.username = username;
        this.comment = comment;
        this.background = background;
    }

    public long getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public String getComment() {
        return comment;
    }

    public String getBackground() {
        return background;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", comment='" + comment + '\'' +
                ", background='" + background + '\'' +
                '}';
    }
}
