import java.io.Serializable;
import java.sql.Timestamp;

public class CityRecord implements Serializable {
    Timestamp dt;
    Double avgTemp;
    Float avgTempUncertainty;
    String city;
    String country;
    String latitude;
    String longitude;

    public CityRecord(Timestamp dt, String avgTemp, String avgTempUncertainty, String city, String country,
                      String latitude, String longitude) {
        setDt(dt);
        if (!avgTemp.equals(""))
            setAvgTemp(new Double(avgTemp));
        else
            this.avgTemp = null;
        if (!avgTempUncertainty.equals(""))
            setAvgTempUncertainty(new Float(avgTempUncertainty));
        else
            this.avgTempUncertainty = null;
        setCity(city);
        setCountry(country);
        setLatitude(latitude);
        setLongitude(longitude);
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

    public String getCity() {
        return city;
    }

    public String getCountry() {
        return country;
    }

    public String getLatitude() {
        return latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setAvgTemp(Double avgTemp) {
        this.avgTemp = avgTemp;
    }

    public void setAvgTempUncertainty(Float avgTempUncertainty) {
        this.avgTempUncertainty = avgTempUncertainty;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setDt(Timestamp dt) {
        this.dt = dt;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
}