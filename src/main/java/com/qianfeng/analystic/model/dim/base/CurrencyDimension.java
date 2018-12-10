package com.qianfeng.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CurrencyDimension extends BaseDimension {

    private int id;
    private String currency_name;

    public CurrencyDimension(int id, String currency_name) {
        this.id = id;
        this.currency_name = currency_name;
    }


    @Override
    public int compareTo(BaseDimension o) {

        if (this==o){
            return 0;
        }

        BaseDimension other = (CurrencyDimension) o;
        int tmp = this.id-((CurrencyDimension) other).id;
        if (tmp!=0){
            return tmp;
        }
        tmp = this.currency_name.compareTo(((CurrencyDimension) other).currency_name);
        return tmp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.currency_name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readInt();
        this.currency_name=dataInput.readUTF();

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrency_name() {
        return currency_name;
    }

    public void setCurrency_name(String currency_name) {
        this.currency_name = currency_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CurrencyDimension that = (CurrencyDimension) o;
        return id == that.id &&
                Objects.equals(currency_name, that.currency_name);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, currency_name);
    }
}
