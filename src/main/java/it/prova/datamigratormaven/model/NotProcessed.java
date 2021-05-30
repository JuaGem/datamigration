package it.prova.datamigratormaven.model;

public class NotProcessed {

    private Long id;
    private String codiceFiscale;
    private Long oldId;

    public NotProcessed() {
    }

    public NotProcessed(String codiceFiscale, Long oldId) {
        this.codiceFiscale = codiceFiscale;
        this.oldId = oldId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCodiceFiscale() {
        return codiceFiscale;
    }

    public void setCodiceFiscale(String codiceFiscale) {
        this.codiceFiscale = codiceFiscale;
    }

    public Long getOldId() {
        return oldId;
    }

    public void setOldId(Long oldId) {
        this.oldId = oldId;
    }

    @Override
    public String toString() {
        return "NotProcessed{" +
                "id=" + id +
                ", codiceFiscale='" + codiceFiscale + '\'' +
                ", oldId=" + oldId +
                '}';
    }
}
