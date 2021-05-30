package it.prova.datamigratormaven.model;

import java.util.Date;

public class Assicurato {

    private Long id;
    private String nome;
    private String cognome;
    private String codiceFiscale;
    private Date data;
    private Integer numeroSinistri;

    public Assicurato() {
    }

    public Assicurato(String nome, String cognome, String codiceFiscale, Date data, Integer numeroSinistri) {
        this.nome = nome;
        this.cognome = cognome;
        this.codiceFiscale = codiceFiscale;
        this.data = data;
        this.numeroSinistri = numeroSinistri;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getCognome() {
        return cognome;
    }

    public void setCognome(String cognome) {
        this.cognome = cognome;
    }

    public String getCodiceFiscale() {
        return codiceFiscale;
    }

    public void setCodiceFiscale(String codiceFiscale) {
        this.codiceFiscale = codiceFiscale;
    }

    public Date getData() {
        return data;
    }

    public void setData(Date data) {
        this.data = data;
    }

    public Integer getNumeroSinistri() {
        return numeroSinistri;
    }

    public void setNumeroSinistri(Integer numeroSinistri) {
        this.numeroSinistri = numeroSinistri;
    }

    @Override
    public String toString() {
        return "Assicurato{" +
                "id=" + id +
                ", nome='" + nome + '\'' +
                ", cognome='" + cognome + '\'' +
                ", codiceFiscale='" + codiceFiscale + '\'' +
                ", data=" + data +
                ", numeroSinistri=" + numeroSinistri +
                '}';
    }
}
