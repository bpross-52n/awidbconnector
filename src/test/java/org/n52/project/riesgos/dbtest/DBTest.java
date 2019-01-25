package org.n52.project.riesgos.dbtest;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.n52.project.riesgos.earthquakesimulation.EarthquakeSimulationDBConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;

public class DBTest {

    private static Logger log = LoggerFactory.getLogger(DBTest.class.getName());
    
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        
        Class.forName("org.postgresql.Driver");
        
        String minLon = args[0];
        String maxLon = args[1];
        String minLat = args[2];
        String maxLat = args[3];
        
        Properties props = new Properties();
        String username = args[4];
        String password = args[5];
        
        String connectionURL = args[6];
        
        EarthquakeSimulationDBConnector earthquakeSimulationDBConnector = new EarthquakeSimulationDBConnector();
        
        earthquakeSimulationDBConnector.connectToDB(connectionURL, username, password);
        
        Map<String, Coordinate> ids = earthquakeSimulationDBConnector.getEpicenterIDsChile(minLon, maxLon, minLat, maxLat);
        
        for (String id : ids.keySet()) {
            log.info(id + " " + ids.get(id));
        }
        
        earthquakeSimulationDBConnector.getIsochronesChile(ids.keySet().iterator().next());   
    }

}
