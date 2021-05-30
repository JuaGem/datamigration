package it.prova.datamigratormaven.service;


import it.prova.datamigratormaven.dao.nuovo.NuovoDAO;
import it.prova.datamigratormaven.dao.vecchio.VecchioDAO;

public class MyServiceFactory {

    public static NuovoService getNuovoService() {
        NuovoService nuovoService = new NuovoService();
        nuovoService.setNuovoDAO(new NuovoDAO());
        return nuovoService;
    }

    public static VecchioService getVecchioService() {
        VecchioService vecchioService = new VecchioService();
        vecchioService.setVecchioDao(new VecchioDAO());
        return vecchioService;
    }


}
