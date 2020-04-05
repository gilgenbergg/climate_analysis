import java.io.Serializable;
import java.sql.Timestamp;

public class CountryRecord implements Serializable {
    Timestamp dt;
    Double avgTemp;
    Float avgTempUncertainty;
    String country;

    public CountryRecord(Timestamp dt, String avgTemp, String avgTempUncertainty, String country) {
        setDt(dt);
        if (!avgTemp.equals(""))
            setAvgTemp(new Double(avgTemp));
        else
            this.avgTemp = null;
        if (!avgTempUncertainty.equals(""))
            setAvgTempUncertainty(new Float(avgTempUncertainty));
        else
            this.avgTempUncertainty = null;
        setCountry(country);
    }

    public Timestamp getDt() {
        return dt;
    }

    public Double getAvgTemp() {
        return avgTemp;
    }

    public Float getAvgTempUncertainty() {
        return avgTempUncertainty;
    }

    public String getCountry() {
        return country;
    }

    public void setAvgTemp(Double avgTemp) {
        this.avgTemp = avgTemp;
    }

    public void setAvgTempUncertainty(Float avgTempUncertainty) {
        this.avgTempUncertainty = avgTempUncertainty;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setDt(Timestamp dt) {
        this.dt = dt;
    }
}
