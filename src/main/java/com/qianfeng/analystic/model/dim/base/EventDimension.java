package com.qianfeng.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class EventDimension extends BaseDimension{

    private int id;
    private String category;
    private String cation;

    public EventDimension(String category, String cation) {
        this.category = category;
        this.cation = cation;
    }

    @Override
    public int compareTo(BaseDimension o) {

        if (this ==o){
            return 0;
        }
        EventDimension br = (EventDimension) o;
        int tmp = this.id-br.id;
        if (tmp !=0){
            return tmp;
        }
        tmp = this.category.compareTo(br.category);
        if (tmp!=0){
            return tmp;
        }
        tmp = this.cation.compareTo(br.cation);

        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.category);
        dataOutput.writeUTF(this.cation);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id=dataInput.readInt();
        this.category=dataInput.readUTF();
        this.cation=dataInput.readUTF();

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCation() {
        return cation;
    }

    public void setCation(String cation) {
        this.cation = cation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventDimension that = (EventDimension) o;
        return id == that.id &&
                Objects.equals(category, that.category) &&
                Objects.equals(cation, that.cation);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, category, cation);
    }

    @Override
    public String toString() {
        return "EventDimension{" +
                "id=" + id +
                ", category='" + category + '\'' +
                ", cation='" + cation + '\'' +
                '}';
    }
}
