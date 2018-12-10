package com.qianfeng.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PayMentDimension extends BrowserDimension {

    private int id;
    private String payment_type;

    public PayMentDimension(int id, String payment_type) {
        this.id = id;
        this.payment_type = payment_type;
    }

    @Override
    public int compareTo(BaseDimension o) {

        if (this==o){
            return 0;
        }
        BaseDimension other = (PayMentDimension) o;

        int tmp = this.id -((PayMentDimension) other).id;
        if (tmp!=0){
            return tmp;
        }
        tmp  = this.payment_type.compareTo(((PayMentDimension) other).payment_type);

        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.payment_type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.payment_type=dataInput.readUTF();
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void setId(int id) {
        this.id = id;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PayMentDimension that = (PayMentDimension) o;
        return id == that.id &&
                Objects.equals(payment_type, that.payment_type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(super.hashCode(), id, payment_type);
    }

    @Override
    public String toString() {
        return "PayMentDimension{" +
                "id=" + id +
                ", payment_type='" + payment_type + '\'' +
                '}';
    }
}
