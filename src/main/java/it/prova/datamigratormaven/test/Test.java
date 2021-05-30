package it.prova.datamigratormaven.test;

import it.prova.datamigratormaven.model.Assicurato;
import it.prova.datamigratormaven.model.NotProcessed;
import it.prova.datamigratormaven.model.Vecchio;
import it.prova.datamigratormaven.service.MyServiceFactory;
import it.prova.datamigratormaven.service.NuovoService;
import it.prova.datamigratormaven.service.VecchioService;

import java.util.List;

public class Test {

    public static void main(String[] args) throws Exception {
        VecchioService vecchioService = MyServiceFactory.getVecchioService();
        List<Vecchio> listaVecchio = vecchioService.listAll();
        NuovoService nuovoService = MyServiceFactory.getNuovoService();


        for (Vecchio elemento : listaVecchio) {

            if (elemento.getCodiceFiscale().length() != 16) {
                NotProcessed notProcessed = new NotProcessed();
                notProcessed.setCodiceFiscale(elemento.getCodiceFiscale());
                notProcessed.setOldId(elemento.getId());
                nuovoService.inserisciNotProcessed(notProcessed);

            } else {
                Assicurato assicurato = new Assicurato();
                assicurato.setNome(elemento.getNome());
                assicurato.setCognome(elemento.getCognome());
                assicurato.setCodiceFiscale(elemento.getCodiceFiscale());
                assicurato.setNumeroSinistri(elemento.getNumeroSinistri());
                assicurato.setData(elemento.getData());
                nuovoService.inserisciAssicurato(assicurato);
            }

        }
    }
}
