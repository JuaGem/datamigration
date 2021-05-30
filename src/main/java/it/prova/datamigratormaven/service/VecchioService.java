package it.prova.datamigratormaven.service;

import it.prova.datamigratormaven.connection.MyConnection;
import it.prova.datamigratormaven.dao.Constants;
import it.prova.datamigratormaven.dao.nuovo.NuovoDAO;
import it.prova.datamigratormaven.dao.vecchio.VecchioDAO;
import it.prova.datamigratormaven.model.Assicurato;
import it.prova.datamigratormaven.model.NotProcessed;
import it.prova.datamigratormaven.model.Vecchio;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class VecchioService {


    private VecchioDAO vecchioDAO;


    public void setVecchioDao(VecchioDAO vecchioDAO) {
        this.vecchioDAO = vecchioDAO;

    }

    public List<Vecchio> listAll() throws Exception {

        List<Vecchio> result = new ArrayList<>();

        try (Connection connection = MyConnection.getConnection(Constants.DRIVER_NAME, Constants.CONNECTION_URL_VECCHIO)) {

            // inietto la connection nel dao
            vecchioDAO.setConnection(connection);

            // eseguo quello che realmente devo fare
            result = vecchioDAO.oldDataInterrogation();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return result;
    }


}
