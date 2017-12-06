package com.epam.bd;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Common utilities
 *  */
public class Utils {
    private static final Logger log = LogManager.getLogger(Utils.class);

    private static final String MISSING_CITY="missing";
    private static final String CITY_LIST_FILE = "city.en.txt";
    private static final String WINDOWS_TAG = "Windows";
    private static Map<Long,String> cityMap = null;

    /**
     * Lookup city name by Id
     * @param cityId
     * @return
     */
    public static String lookupCity(long cityId) {
        synchronized(Utils.class) {
            if (cityMap == null ) {
                cityMap = readCityList();
            }
        }

        Long id = Long.valueOf(cityId);
        return cityMap.get(id) == null ?
                MISSING_CITY + "(" + String.valueOf(cityId) + ")"
                : cityMap.get(id);
    }

    /**
     * Read from resources and cache city list
     * @return
     */
    private static Map<Long,String> readCityList() {
        Map<Long,String> map = new HashMap<Long,String>();
        ClassLoader classLoader = Utils.class.getClassLoader();
        InputStream cityList = classLoader.getResourceAsStream(CITY_LIST_FILE);

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(cityList));

            String line = reader.readLine();
            while(line != null){
                String[] items = line.split("[ \t]");
                map.put(Long.valueOf(items[0]), items[1]);
                line = reader.readLine();
            }
        } catch(IOException ex){
            log.error("Cannot read city file", ex);
        }

        return map;
    }

    /**
     * Parses line and sets entry fields
     * @param entry
     * @param rawLine
     */
    public static void parseLine(ImpressionEntry entry, String rawLine)
    {
        String[] items = rawLine.split("\t");
        try {
            entry.set(
                    Long.parseLong(items[7]), //CityID
                    Integer.parseInt(items[19]), //Bidding Price
                    1, //total records for combining
                    items[4]); //User Agent
        } catch(NumberFormatException ex) {
            log.error("Error converting line", ex);
            entry.set(-1, 0,0, "");
        }
    }

    /**
     * Propose partition id based on user agent string. Windows: 0, all others: 1
     * @param userAgent
     * @return
     */
    public static int getPartitionId(String userAgent) {
        return userAgent.indexOf(WINDOWS_TAG) >= 0 ? 0 : 1;
    }
}
