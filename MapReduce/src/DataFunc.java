
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author mike this class is used to handle all of the data manipulations
 */
public class DataFunc {

	static List<List<String>> AComp_Passenger_data = new ArrayList<List<String>>(6); // declare list of lists
	static List<List<String>> Top30_Airports_data = new ArrayList<List<String>>(4); // declare list of lists
	static List<List<String>> flightList = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> distanceList = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> allFlights = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> distancePerPerson = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> numTripsPerDest = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> numPassengersPerFlight = new ArrayList<List<String>>(); // declare list of lists
	static List<String> LocationsWithNoTrips = new ArrayList<String>(); // declare list of locations with no trips

	/**
	 * @author mike prints out table
	 */

	public void preprocess() {

		removeMissingValues(AComp_Passenger_data); // removes rows containing missing values
		removecorruptRows();
		LocationsWithNoTrips = getLocationsWithNoTrips();
		numTripsPerDest = numTripsPerDest();
		flightList = calcFlightList();
		distancePerPerson = distancePerPerson();
		for (int i = 0; i < AComp_Passenger_data.size(); i++)
			makeColumnUppercase(i); // make all columns uppercase
		printAllData();

	}

	/**
	 * @author mike Finds passenger with the most air miles
	 * @return
	 */

	public List<String> mostMilesPas() {
		double largestNum = 0;
		int largestNumRow = 0;
		List<String> mostMPass = new ArrayList<String>(); // declare list of locations with no trips
		for (int distPP = 0; distPP < distancePerPerson.get(0).size(); distPP++) {

			double temp = Double.parseDouble(distancePerPerson.get(1).get(distPP));
			if (temp > largestNum) { // if largest number found so far
				largestNum = temp;
				largestNumRow = distPP;
			}
		}
		mostMPass.add(distancePerPerson.get(0).get(largestNumRow));
		mostMPass.add(distancePerPerson.get(1).get(largestNumRow));
		return mostMPass;

	}

	/**
	 * @author mike prints out all data
	 */

	public void printAllData() {

		System.out.println("\n\nAComp Passenger data:\n");
		printTable(AComp_Passenger_data); // prints the data

		System.out.println("\n\nTop 30 airport data:\n");
		printTable(Top30_Airports_data); // prints the data

		System.out.println("\n\nFlight List data:\n");
		printTable(getFlightList());

		System.out.println("\n\nnumber of trips per destination:\n");
		printTable(getNumFlights());

		System.out.println("\n\nAirports with no flights:\n");
		System.out.println(getAPNoFlight());

		System.out.println("\n\nNumber of passengers per flight:\n");
		printTable(getNumPassenger());

		System.out.println("\n\nnautical miles travelled per flight:\n");
		printTable(getFlightMiles());

		System.out.println("\n\nTotal miles travelled per passenger:\n");
		printTable(getDistancePerPerson());

		System.out.println("\n\nPassenger with most air miles:\n");
		System.out.println(mostMilesPas());

	}

	public DataFunc(List<List<String>> data1, List<List<String>> data2) {
		AComp_Passenger_data = data1;
		Top30_Airports_data = data2;
	}

	/**
	 * @author mike prints out table
	 */

	public static void printTable(List<List<String>> data) {

		int numColumns = data.size(); // gets number of columns from table
		int numRows = data.get(0).size(); // gets number of rows from table
		// prints out table:
		for (int row = 0; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++)
				System.out.print(data.get(column).get(row) + "\t");
			System.out.print("\t\trow num:" + row + "\n");
		}
	}

	/**
	 * @author mike "return distance returns the birds eye distance
	 */
	public static double distanceTrav(double long1, double lat1, double long2, double lat2) {
		double num1 = (long1 - long2) * (long1 - long2);
		double num2 = (lat1 - lat2) * (lat1 - lat2);
		return Math.sqrt(num1 + num2);
	}

	/**
	 * @author mike removes all rows that contain missing values
	 */
	public static void removeMissingValues(List<List<String>> data) {

		int numColumns = data.size(); // gets number of columns from table
		int numRows = data.get(0).size(); // gets number of rows from table

		// removes values
		for (int row = 0; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++) {
				// if a column on that row is empty or 0 then remove row (could change to a huge
				// if statement with checking all columns at once)
				if (data.get(column).get(row).contentEquals("0") || data.get(column).get(row).isEmpty()) {
					for (int col = 0; col < numColumns; col++)
						data.get(col).remove(row); // for all columns on that row remove data
					numRows--; // reduces the number of rows based on how many have been removed
					break; // breaks out of for loop so it doesn't check other columns and go out of index
							// on final row
				}
			}
		}

	}

	/**
	 * @author mike removes all rows that contain missing values
	 */
	public static void removecorruptRows() {

		int numColumns = AComp_Passenger_data.size(); // gets number of columns from table
		int numRows = AComp_Passenger_data.get(0).size(); // gets number of rows from table

		// removes values
		for (int row = 0; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++) {

				String Temp = AComp_Passenger_data.get(column).get(row);
				if (Temp.contains("|") || Temp.contains("^") || Temp.contains(":") || Temp.contains("~")
						|| Temp.contains(";") || Temp.contains("}") || Temp.contains("#") || Temp.contains("%")
						|| Temp.contains("[") || Temp.contains("]")) { // if a column on that row is corrupted
					for (int col = 0; col < numColumns; col++)
						AComp_Passenger_data.get(col).remove(row); // for all columns on that row remove data
					numRows--; // reduces the number of rows based on how many have been removed
					break; // breaks out of for loop so it doesn't check other columns and go out of index
							// on final row
				}
			}
		}

	}

	/**
	 * @author mike makes a column uppercase
	 */
	public static void makeColumnUppercase(int columnNum) {
		int numRows = AComp_Passenger_data.get(0).size(); // gets number of rows from table
		// for all rows set column to an uppercase version of itself
		for (int row = 0; row < numRows; row++)
			AComp_Passenger_data.get(columnNum).set(row, AComp_Passenger_data.get(columnNum).get(row).toUpperCase());

	}

	/**
	 * @author mike turns epoch time to a date string
	 */
	public static void epochToDate(List<List<String>> data, DateFormat dateFormat) {

		int numRows = data.get(0).size(); // gets number of rows from table
		List<Date> dateArr = new ArrayList<Date>();
		// changes epoch to date:
		for (int row = 0; row < numRows; row++) {

			String epochString = data.get(4).get(row);
			long epochTime = Long.parseLong(epochString);
			Date D1 = new Date(epochTime * 1000);
			dateArr.add(D1);
			String strDate = dateFormat.format(D1);
			data.get(4).set(row, strDate);

			String epochString2 = data.get(5).get(row);
			long epochTime2 = Long.parseLong(epochString2);
			Date D2 = new Date((epochTime2 * 60000) + (epochTime * 1000));
			dateArr.add(D2);
			String strDate2 = dateFormat.format(D2);
			data.get(5).set(row, strDate2);

		}

	}

	/**
	 * @author mike
	 */
	public static List<String> getLocationsWithNoTrips() {
		List<String> tempArr = new ArrayList<String>(AComp_Passenger_data.size());
		List<String> noTrips = new ArrayList<String>();

		tempArr.addAll(AComp_Passenger_data.get(2)); // list of all starting locations
		tempArr.addAll(AComp_Passenger_data.get(3)); // list of all destinations
		Set<String> uniqueDest = new HashSet<String>(tempArr);// gets all unique destinations

		// if the airport has not been on a trip add it to the notrips list
		for (String dest1 : Top30_Airports_data.get(1)) {
			if (!uniqueDest.contains(dest1))
				noTrips.add(dest1);
		}
		return noTrips;

	}

	/**
	 * @author mike calculates number of trips (to + from) and destination
	 * @return
	 */
	public static List<List<String>> numTripsPerDest() {
		List<String> tempArr = new ArrayList<String>(AComp_Passenger_data.size());
		List<List<String>> trips = new ArrayList<List<String>>(2); // declare list of lists
		// instantiate all columns
		trips.add(new ArrayList<String>());
		trips.add(new ArrayList<String>());

		tempArr.addAll(AComp_Passenger_data.get(2)); // list of all starting locations
		tempArr.addAll(AComp_Passenger_data.get(3)); // list of all destinations
		int occurrences = 0;
		Set<String> uniqueDest = new HashSet<String>(tempArr);// gets all unique destinations

		for (String myValue : uniqueDest) {
			occurrences = (Collections.frequency(AComp_Passenger_data.get(2), myValue)
					+ Collections.frequency(AComp_Passenger_data.get(3), myValue));
			trips.get(0).add(myValue);
			trips.get(1).add(Integer.toString(occurrences));
		}

		return trips;
	}

	/**
	 * @author mike
	 */
	public List<List<String>> calcFlightList() {
		List<List<String>> flights = new ArrayList<List<String>>(6); // 2D array (column,row)
		List<List<String>> numPassengers = new ArrayList<List<String>>(2); // 2D array (column,row)
		List<List<String>> flightsWOutPassIDEct = new ArrayList<List<String>>(); // array for flight relevant info
																					// only//TODO
		// initiate numPassengers
		numPassengers.add(new ArrayList<String>());
		numPassengers.add(new ArrayList<String>());

		// initiate flights
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		// initiate flightsWOutPassIDEct
		flightsWOutPassIDEct.add(new ArrayList<String>());
		flightsWOutPassIDEct.add(new ArrayList<String>());
		flightsWOutPassIDEct.add(new ArrayList<String>());
		flightsWOutPassIDEct.add(new ArrayList<String>());
		flightsWOutPassIDEct.add(new ArrayList<String>());

		Set<String> uniqueFlight = new HashSet<String>(AComp_Passenger_data.get(1));// gets all unique flights

		DateFormat dateFormat = new SimpleDateFormat("HH:mm dd/MM/yyyy"); // defines the date format (matches No Error
																			// dataset)

		epochToDate(AComp_Passenger_data, dateFormat); // runs method epoch to date to get the departure and arrival
														// times

		// calculate the full flight list (NOT SURE IF THIS IS WHAT THE SPEC WAS ASKING
		// FOR ALTHOUGH I THOUGHT THE SMALLER LIST WAS BETTER SUITED)
		for (String flight : uniqueFlight) {
			int count = 0;
			for (int row = 0; row < AComp_Passenger_data.get(0).size(); row++) {
				if (flight.equals(AComp_Passenger_data.get(1).get(row))) {
					count++;
					flights.get(0).add(AComp_Passenger_data.get(1).get(row)); // add flight ID
					flights.get(1).add(AComp_Passenger_data.get(0).get(row)); // add passenger ID
					flights.get(2).add(AComp_Passenger_data.get(2).get(row)); // add from Airport
					flights.get(3).add(AComp_Passenger_data.get(3).get(row)); // add to airport
					flights.get(4).add(AComp_Passenger_data.get(4).get(row)); // add departure time
					flights.get(5).add(AComp_Passenger_data.get(5).get(row)); // add arrival time
				}
			}
			numPassengers.get(0).add(flight); // add flightID
			numPassengers.get(1).add(Integer.toString(count)); // add number of passengers per flight
		}
		// calculate the flight list
		for (String flight : uniqueFlight) {
			for (int row = 0; row < AComp_Passenger_data.get(0).size(); row++) {
				if (flight.equals(AComp_Passenger_data.get(1).get(row))) {
					flightsWOutPassIDEct.get(0).add(AComp_Passenger_data.get(1).get(row)); // add flight ID
					flightsWOutPassIDEct.get(1).add(AComp_Passenger_data.get(2).get(row)); // add from Airport
					flightsWOutPassIDEct.get(2).add(AComp_Passenger_data.get(3).get(row)); // add to airport
					flightsWOutPassIDEct.get(3).add(AComp_Passenger_data.get(4).get(row)); // add departure time
					flightsWOutPassIDEct.get(4).add(AComp_Passenger_data.get(5).get(row)); // add arrival time
					break;
				}
			}
		}

		distanceList = getFlightDistList(flightsWOutPassIDEct); // this method returns list of distances per flight
		allFlights = flights; // set all flights to flights
		numPassengersPerFlight = numPassengers; // sets numPassengers PerFlight to numPassengers
		return flightsWOutPassIDEct; // returns short list of flights (without passenger ID)

	}

	/**
	 * @author mike
	 * @param flightslist
	 * @return flightDistances returns distances for each flight
	 */
	public List<List<String>> getFlightDistList(List<List<String>> flightslist) {
		List<List<String>> flights = new ArrayList<List<String>>(5); // 2D array (column,row)
		List<List<String>> flightDistances = new ArrayList<List<String>>(2); // 2D array (column,row)

		// Initialise flights
		flights.add(flightslist.get(0));
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flights.add(new ArrayList<String>());
		flightDistances.add(new ArrayList<String>());
		flightDistances.add(new ArrayList<String>());

		// for each flight find the long and lat of destination and starting airport
		for (int flight = 0; flight < flights.get(0).size(); flight++) {
			for (int airPorts = 0; airPorts < Top30_Airports_data.get(0).size(); airPorts++) {
				// if the airport has a long and lat add it to the flights list
				if (flightslist.get(1).get(flight).equals(Top30_Airports_data.get(1).get(airPorts))) {
					flights.get(1).add(Top30_Airports_data.get(2).get(airPorts));
					flights.get(2).add(Top30_Airports_data.get(3).get(airPorts));
				}
				// if the airport has a long and lat add it to the flights list
				if (flightslist.get(2).get(flight).equals(Top30_Airports_data.get(1).get(airPorts))) {
					flights.get(3).add(Top30_Airports_data.get(2).get(airPorts));
					flights.get(4).add(Top30_Airports_data.get(3).get(airPorts));
				}
			}

		}

		// for all flights
		for (int flight = 0; flight < flights.get(0).size(); flight++) {
			// calculate distance traveled with long and lat
			double Temp = distanceTrav(Double.parseDouble(flights.get(1).get(flight)),
					Double.parseDouble(flights.get(2).get(flight)), Double.parseDouble(flights.get(3).get(flight)),
					Double.parseDouble(flights.get(4).get(flight)));
			flightDistances.get(0).add(flight, flights.get(0).get(flight));// add flight name
			flightDistances.get(1).add(flight, Double.toString(Temp)); // add distance

		}

		return flightDistances; // return flight distances

	}

	/**
	 * @author mike calculates distance travelled per person
	 */

	public static List<List<String>> distancePerPerson() {
		List<List<String>> DistPP = new ArrayList<List<String>>(2); // distance per person

		Set<String> uniquePass = new HashSet<String>(allFlights.get(1));// gets all unique passengerID's
		DistPP.add(new ArrayList<String>());
		DistPP.add(new ArrayList<String>());

		// for all passengers
		for (String passID : uniquePass) {
			double TotalDist = 0;
			// for all flights
			for (int i = 0; i < allFlights.get(0).size(); i++) {
				// if the passenger is on that flight
				if (allFlights.get(1).get(i).equals(passID)) {
					// for all flight Distances
					for (int flightDistNum = 0; flightDistNum < distanceList.get(0).size(); flightDistNum++) {
						if (distanceList.get(0).get(flightDistNum).equals(allFlights.get(0).get(i))) {
							String temp = distanceList.get(1).get(flightDistNum);
							TotalDist += Double.parseDouble(temp);
						}

					}

				}
			}

			DistPP.get(0).add(passID);
			DistPP.get(1).add(Double.toString(TotalDist));
		}

		return DistPP;
	}

	// Getters:

	/**
	 * @author mike getter for airports that have not been used
	 * @return LocationsWithNoTrips
	 */
	public List<String> getAPNoFlight() {
		return LocationsWithNoTrips;

	}

	/**
	 * @author mike getter for nautical miles for each flight
	 * @return distanceList
	 */
	public List<List<String>> getFlightMiles() {
		return distanceList;

	}

	/**
	 * @author mike getter for list of flights
	 * @return FlightList
	 */
	public List<List<String>> getFlightList() {
		return flightList;

	}

	/**
	 * @author mike getter for number of passengers per flight
	 * @return numPassengersPerFlight
	 */
	public List<List<String>> getNumPassenger() {
		return numPassengersPerFlight;

	}

	/**
	 * @author mike getter for number of trips per destination
	 * @return numTripsPerDest
	 */
	public List<List<String>> getNumFlights() {
		return numTripsPerDest;

	}

	/**
	 * @author mike getter for distance travelled per person
	 * @return distancePerPerson
	 */
	public List<List<String>> getDistancePerPerson() {
		return distancePerPerson;

	}

	/**
	 * @author mike getter for main dataset
	 * @return AComp_Passenger_data
	 */
	public List<List<String>> getAComp_Passenger_data() {
		return AComp_Passenger_data;
	}
}
