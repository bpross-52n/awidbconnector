package org.n52.project.riesgos.earthquakesimulation;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;

public class EarthquakeSimulationDBConnector {

    private static Logger log = LoggerFactory.getLogger(EarthquakeSimulationDBConnector.class.getName());

    private Connection connection;

    public EarthquakeSimulationDBConnector() {

    }

    public boolean connectToDB(String connectionURL,
            String username,
            String password) throws SQLException, ClassNotFoundException {
        
        Class.forName("org.postgresql.Driver");

        Properties props = new Properties();

        props.setProperty("user", username);
        props.setProperty("password", password);

        Enumeration<Driver> drivers = DriverManager.getDrivers();
        
        while(drivers.hasMoreElements()){
            log.debug(drivers.nextElement().toString());
        }
        
        connection = DriverManager.getConnection(connectionURL, props);

        return connection.isValid(5000);
    }

    public Map<String, Coordinate> getEpicenterIDs(String minLon,
            String maxLon,
            String minLat,
            String maxLat) throws SQLException {

        String sql = String.format(
                "SELECT * FROM epicenters WHERE (longitude BETWEEN %s AND %s) AND (latitude BETWEEN %s AND %s)", minLon,
                maxLon, minLat, maxLat);

        PreparedStatement preparedStatement =
                connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        preparedStatement.execute();

        ResultSet resultSet = preparedStatement.getResultSet();

        Map<String, Coordinate> idList = new HashMap<String, Coordinate>();

        while (resultSet.next()) {

            double longitude = resultSet.getDouble(3);
            double latitude = resultSet.getDouble(4);
            
            Coordinate coordinate = new Coordinate(longitude, latitude);
            
            String mags = resultSet.getString(8);
            String epi_i = resultSet.getString(9);
            String epi_j = resultSet.getString(10);

            log.info("Got result with mags: " + mags + ", epi_i: " + epi_i + " and epi_j: " + epi_j);

            String id = mags + epi_j + epi_i;

            log.info("Forming id: " + id);

            idList.put(id, coordinate);
        }

        return idList;
    }
    
    public Map<String, Coordinate> getEpicenterIDsChile(String minLon,
            String maxLon,
            String minLat,
            String maxLat) throws SQLException {
        
        String sql = String.format(
                "SELECT * FROM riesgos_app_poi WHERE (\"Lon\" BETWEEN %s AND %s) AND (\"Lat\" BETWEEN %s AND %s)", minLon,
                maxLon, minLat, maxLat);
        
        PreparedStatement preparedStatement =
                connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        
        preparedStatement.execute();
        
        ResultSet resultSet = preparedStatement.getResultSet();
        
        Map<String, Coordinate> idList = new HashMap<String, Coordinate>();
        
        while (resultSet.next()) {
            
            double longitude = resultSet.getDouble(4);
            double latitude = resultSet.getDouble(3);
            
            Coordinate coordinate = new Coordinate(longitude, latitude);
            
//            String mags = resultSet.getString(8);
//            String epi_i = resultSet.getString(9);
            String id = resultSet.getString(8);
            
//            log.info("Got result with mags: " + mags + ", epi_i: " + epi_i + " and epi_j: " + epi_j);
//            
//            String id = mags + epi_j + epi_i;
            
            log.info("Scenario id: " + id);
            
            idList.put(id, coordinate);
        }
        
        return idList;
    }

    public Map<String, String> getIsochrones(String id) throws SQLException {
        
        String sql = String.format("SELECT * FROM tsunami_wave_isochrones30min WHERE id = %s", id);

        PreparedStatement preparedStatement =
                connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        preparedStatement.execute();

        ResultSet resultSet = preparedStatement.getResultSet();

        Map<String, String> timeStampIsochroneMap = new HashMap<String, String>();
        
        while (resultSet.next()) {

            String arrivalTime = resultSet.getString(2);
            String st_geometry = resultSet.getString(4);
            
            log.info("Got result with first arrival time: " + arrivalTime + " and shape: " + st_geometry);
            
            String geometryAstext = getST_GeometryAsText(st_geometry);
            
            log.info("ST_Geometry as text: " + geometryAstext);
            
            timeStampIsochroneMap.put(arrivalTime, geometryAstext);
        }

        return timeStampIsochroneMap;
    }
    
    public List<String> getIsochronesChile(String id) throws SQLException {
        
        String sql = String.format("SELECT * FROM riesgos_app_isochron WHERE \"scenario_id\" = %s", id);
        
        PreparedStatement preparedStatement =
                connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        
        preparedStatement.execute();
        
        ResultSet resultSet = preparedStatement.getResultSet();
        
        List<String> timeStampIsochroneMap = new ArrayList<String>();
        
        while (resultSet.next()) {
            
            String geojson = resultSet.getString(2);
//            String st_geometry = resultSet.getString(4);
            
            log.info("Got result with shape " + geojson);
            
//            String geometryAstext = getST_GeometryAsText(st_geometry);
            
//            log.info("ST_Geometry as text: " + geometryAstext);
            
            timeStampIsochroneMap.add(geojson);
        }
        
        return timeStampIsochroneMap;
    }
    
    private String getST_GeometryAsText(String st_geometry) throws SQLException{
        
        String sql = String.format("SELECT sde.st_astext(st_geometry('%s'))", st_geometry);

        PreparedStatement preparedStatement =
                connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        preparedStatement.execute();

        ResultSet resultSet = preparedStatement.getResultSet();
        
        resultSet.next();
        
        return resultSet.getString(1);
        
    }

}
