package it.prova.datamigratormaven.service;

import it.prova.datamigratormaven.connection.MyConnection;
import it.prova.datamigratormaven.dao.Constants;
import it.prova.datamigratormaven.dao.nuovo.NuovoDAO;
import it.prova.datamigratormaven.dao.vecchio.VecchioDAO;
import it.prova.datamigratormaven.model.Assicurato;
import it.prova.datamigratormaven.model.NotProcessed;

import java.sql.Connection;

public class NuovoService {

    private NuovoDAO nuovoDAO;


    public void setNuovoDAO(NuovoDAO nuovoDAO) {
        this.nuovoDAO = nuovoDAO;

    }


    public int inserisciAssicurato(Assicurato input) throws Exception {
        if (input == null)
            throw new Exception("Valore di input non ammesso.");


        int result = 0;
        try (Connection connection = MyConnection.getConnection(Constants.DRIVER_NAME, Constants.CONNECTION_URL_NUOVO)) {

            // inietto la connection nel dao
            nuovoDAO.setConnection(connection);

            // eseguo quello che realmente devo fare
            result = nuovoDAO.insertAssicurato(input);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return result;
    }


    public int inserisciNotProcessed(NotProcessed input) throws Exception {
        if (input == null)
            throw new Exception("Valore di input non ammesso.");


        int result = 0;
        try (Connection connection = MyConnection.getConnection(Constants.DRIVER_NAME, Constants.CONNECTION_URL_NUOVO)) {

            nuovoDAO.setConnection(connection);

            result = nuovoDAO.insertNotProcessed(input);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return result;
    }
}
