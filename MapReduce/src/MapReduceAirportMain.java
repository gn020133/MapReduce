import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author mike this class is used to read in the files, create threads and
 *         reduce all of the data. it also saves files and prints final results
 */
public class MapReduceAirportMain {

	static List<List<String>> AComp_Passenger_data = new ArrayList<List<String>>(6); // declare list of lists
	static List<List<String>> Top30_Airports_data = new ArrayList<List<String>>(4); // declare list of lists

	static List<List<String>> AComp_Passenger_data_new = new ArrayList<List<String>>(6); // declare list of lists
	static List<List<String>> flightList = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> distanceList = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> allFlights = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> distancePerPerson = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> numTripsPerDest = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> numPassengersPerFlight = new ArrayList<List<String>>(); // declare list of lists
	static List<List<String>> mostMilesPassengers = new ArrayList<List<String>>(); // declare list of lists
	static List<String> LocationsWithNoTrips = new ArrayList<String>(); // declare list of locations with no trips
	static List<String> mostMilesPassenger = new ArrayList<String>(); // declare list of locations with no trips

	public static int finishedThreads = 0;
	// declare all threads
	static Threader T1 = new Threader("T1");
	static Threader T2 = new Threader("T2");
	static Threader T3 = new Threader("T3");
	static Threader T4 = new Threader("T4");

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		// data for AComp_Passenger
		String AComp_PassengerFName = "C:\\Users\\mike\\Downloads\\AComp_Passenger_data(2).csv"; // TODO Change me!!!!!!
																									// (main data file
																									// path)
		int AComp_PassengerNumRows = 510;
		int AComp_PassengerNumColumns = 6;

		// data for Top30_Airports
		String Top30_AirportsFName = "C:\\Users\\mike\\Downloads\\Top30_airports_LatLong(1).csv"; // TODO Change
																									// me!!!!!(top30
																									// data file path)
		int Top30_AirportsNumRows = 31;
		int Top30_AirportsNumColumns = 4;

		String saveFileN = "C:\\Users\\mike\\Downloads\\output.csv"; // TODO Change me!!!!!(file to be saved file path)

		// read data into lists of lists
		AComp_Passenger_data = readFile(AComp_PassengerFName, AComp_PassengerNumRows, AComp_PassengerNumColumns); // read
																													// AComp_Passenger_data
																													// into
																													// AComp_Passenger_data
																													// 2d
																													// list
		Top30_Airports_data = readFile(Top30_AirportsFName, Top30_AirportsNumRows, Top30_AirportsNumColumns);// read
																												// Top30_Airports_data
																												// into
																												// Top30_Airports_data
																												// 2d
																												// list

		// the first char is a ? remove it
		AComp_Passenger_data.get(0).set(0, AComp_Passenger_data.get(0).get(0).substring(1));
		List<List<List<String>>> allSubsets = splitListFour(AComp_Passenger_data);

		runThreads(allSubsets, Top30_Airports_data); // run all 4 threads

		// sleep for 1 second
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// if all 4 threads are finished running
		if (finishedThreads == 4) {
			System.out.println("all Threads fin");
			reduceDataFromThreads(); // combine and reduce data from threads
			printAllData(); // print all data to the console
			saveTable(AComp_Passenger_data_new, saveFileN); // save the main data table
		}

	}

	/**
	 * @author mike Increment num of finished threads by 1
	 */
	public static void finishedThread() {
		finishedThreads++; // Increment num of finished threads
	}

	/**
	 * @author mike combine and reduce data from all threads
	 */
	public static void reduceDataFromThreads() {

		// initialize all lists
		for (int col = 0; col < 6; col++)
			AComp_Passenger_data_new.add(new ArrayList<String>());
		for (int col = 0; col < 5; col++)
			flightList.add(new ArrayList<String>());
		for (int col = 0; col < 2; col++)
			numTripsPerDest.add(new ArrayList<String>());
		for (int col = 0; col < 2; col++)
			numPassengersPerFlight.add(new ArrayList<String>());
		for (int col = 0; col < 2; col++)
			distanceList.add(new ArrayList<String>());
		for (int col = 0; col < 2; col++)
			distancePerPerson.add(new ArrayList<String>());
		for (int col = 0; col < 2; col++)
			mostMilesPassengers.add(new ArrayList<String>());

		// collect AComp passenger data
		for (int col = 0; col < AComp_Passenger_data_new.size(); col++) {
			AComp_Passenger_data_new.get(col).addAll(T1.df.getAComp_Passenger_data().get(col));
			AComp_Passenger_data_new.get(col).addAll(T2.df.getAComp_Passenger_data().get(col));
			AComp_Passenger_data_new.get(col).addAll(T3.df.getAComp_Passenger_data().get(col));
			AComp_Passenger_data_new.get(col).addAll(T4.df.getAComp_Passenger_data().get(col));
		}

		// collect flight list data
		for (int col = 0; col < flightList.size(); col++) {
			flightList.get(col).addAll(T1.df.getFlightList().get(col));
			flightList.get(col).addAll(T2.df.getFlightList().get(col));
			flightList.get(col).addAll(T3.df.getFlightList().get(col));
			flightList.get(col).addAll(T4.df.getFlightList().get(col));
		}

		// collect flights per airport data /TODO (add duplicates together)
		for (int col = 0; col < numTripsPerDest.size(); col++) {
			numTripsPerDest.get(col).addAll(T1.df.getNumFlights().get(col));
			numTripsPerDest.get(col).addAll(T2.df.getNumFlights().get(col));
			numTripsPerDest.get(col).addAll(T3.df.getNumFlights().get(col));
			numTripsPerDest.get(col).addAll(T4.df.getNumFlights().get(col));
		}

		// collect number of passenger data
		for (int col = 0; col < numPassengersPerFlight.size(); col++) {
			numPassengersPerFlight.get(col).addAll(T1.df.getNumPassenger().get(col));
			numPassengersPerFlight.get(col).addAll(T2.df.getNumPassenger().get(col));
			numPassengersPerFlight.get(col).addAll(T3.df.getNumPassenger().get(col));
			numPassengersPerFlight.get(col).addAll(T4.df.getNumPassenger().get(col));
		}

		// collect data for airports with no flight (to or from) //TODO need to verify
		// that the airports are not used in every thread(contains 4)then reduce
		LocationsWithNoTrips.addAll(T1.df.getAPNoFlight());
		LocationsWithNoTrips.addAll(T2.df.getAPNoFlight());
		LocationsWithNoTrips.addAll(T3.df.getAPNoFlight());
		LocationsWithNoTrips.addAll(T4.df.getAPNoFlight());

		// collect miles per flight
		for (int col = 0; col < distanceList.size(); col++) {
			distanceList.get(col).addAll(T1.df.getFlightMiles().get(col));
			distanceList.get(col).addAll(T2.df.getFlightMiles().get(col));
			distanceList.get(col).addAll(T3.df.getFlightMiles().get(col));
			distanceList.get(col).addAll(T4.df.getFlightMiles().get(col));
		}

		// collect miles per flight
		for (int col = 0; col < distancePerPerson.size(); col++) {
			distancePerPerson.get(col).addAll(T1.df.getDistancePerPerson().get(col));
			distancePerPerson.get(col).addAll(T2.df.getDistancePerPerson().get(col));
			distancePerPerson.get(col).addAll(T3.df.getDistancePerPerson().get(col));
			distancePerPerson.get(col).addAll(T4.df.getDistancePerPerson().get(col));
		}

		//// most miles travelled per passenger WOULD NOT WORK 100% AS 1 PASSENGER COULD
		//// BE IN TWO DIFFERENT SUBSETS
		for (int col = 0; col < mostMilesPassengers.size(); col++) {
			mostMilesPassengers.get(col).add(T1.df.mostMilesPas().get(col));
			mostMilesPassengers.get(col).add(T2.df.mostMilesPas().get(col));
			mostMilesPassengers.get(col).add(T3.df.mostMilesPas().get(col));
			mostMilesPassengers.get(col).add(T4.df.mostMilesPas().get(col));
		}

		// reduce the most miles travelled dataset to the passenger with the highest air
		// mileage
		double largestNum = 0; // longest distance so far
		int largestNumRow = 0;// index
		List<String> mostMPass = new ArrayList<String>(); // declare list of locations with no trips
		for (int distPP = 0; distPP < mostMilesPassengers.get(0).size(); distPP++) {

			double temp = Double.parseDouble(mostMilesPassengers.get(1).get(distPP));
			if (temp > largestNum) { // if largest number found so far
				largestNum = temp; // new longest distance is recorded
				largestNumRow = distPP; // index is recorded
			}
		}
		mostMPass.add(distancePerPerson.get(0).get(largestNumRow)); // adds data to temp array
		mostMPass.add(distancePerPerson.get(1).get(largestNumRow));// adds data to temp array
		mostMilesPassenger = mostMPass; // sets global var to answer
	}

	/**
	 * @author mike
	 * @param fileName
	 *            string for the file path+name
	 * @param data
	 *            2d list of data saves the table in csv format
	 */
	public static void saveTable(List<List<String>> data, String fileName) throws IOException {

		final String lineSeparator = System.getProperty("line.separator");

		// setup the header line
		StringBuilder stringBuildr = new StringBuilder();
		stringBuildr.append(lineSeparator);

		// now append your data in a loop
		for (int row = 0; row < data.get(0).size(); row++) {
			for (int col = 0; col < data.size(); col++) {
				stringBuildr.append(data.get(col).get(row));
				stringBuildr.append(",");

			}
			stringBuildr.append(lineSeparator);

		}
		// now write to file
		Files.write(Paths.get(fileName), stringBuildr.toString().getBytes());
	}

	/**
	 * @author mike prints out all data
	 */

	public static void printAllData() {

		System.out.println("\n\nAComp Passenger data:\n");
		printTable(AComp_Passenger_data); // prints the data

		System.out.println("\n\nTop 30 airport data:\n");
		printTable(Top30_Airports_data); // prints the data

		System.out.println("\n\nFlight List data:\n");
		printTable(flightList);

		System.out.println("\n\nnumber of trips per destination:\n");
		printTable(numTripsPerDest);

		System.out.println("\n\nAirports with no flights:\n");
		System.out.println(LocationsWithNoTrips);

		System.out.println("\n\nNumber of passengers per flight:\n");
		printTable(numPassengersPerFlight);

		System.out.println("\n\nnautical miles travelled per flight:\n");
		printTable(distanceList);

		System.out.println("\n\nTotal miles travelled per passenger:\n");
		printTable(distancePerPerson);

		System.out.println("\n\nPassenger with most air miles:\n");
		System.out.println(mostMilesPassenger);

	}

	/**
	 * @author mike prints out 2d table * @param data 2d list of data
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
	 * @author mike runs 4 threads
	 * @param allSubsets
	 *            3d array containing 4 2d arrays
	 * @param tp30Data
	 *            2d array containing top30 airports Data
	 */
	public static void runThreads(List<List<List<String>>> allSubsets, List<List<String>> tp30Data) {

		// start all threads (FOR WHATEVER REASON THE THREADS ARE NOT WORKING TOGETHER
		// SO ADDED DELAY MUST FIX!!!!!)
		T1.start(allSubsets.get(0), tp30Data);
		try {
			TimeUnit.SECONDS.sleep(1); // DELAY
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		T2.start(allSubsets.get(1), tp30Data);
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		T3.start(allSubsets.get(2), tp30Data);
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		T4.start(allSubsets.get(3), tp30Data);
	}

	/**
	 * @author mike reads file to a 2D list
	 * @param fName
	 *            this is the location and name of the file
	 * @param mumRows
	 *            this is the number of rows in the table
	 * @param mumColumns
	 *            this is the number of columns in the table reads file and puts it
	 *            into a list of lists
	 */
	public static List<List<String>> readFile(String fName, int numRows, int numColumns) {
		List<List<String>> lol = new ArrayList<List<String>>(numColumns); // declare list of lists
		// instantiate all columns
		for (int i = 0; i < numColumns; i++)
			lol.add(new ArrayList<String>());

		String[] tempArray;

		try {
			BufferedReader buffr = Files.newBufferedReader(Paths.get(fName));
			for (int row = 0; row < numRows; row++) {
				// read data from the buffered reader
				tempArray = buffr.readLine().split(",");
				// if the temporary has the correct amount of columns add the data to the list
				// of lists
				if (tempArray.length == numColumns)
					for (int column = 0; column < numColumns; column++)
						lol.get(column).add(tempArray[column]);
			}

		} catch (IOException io) {
			io.printStackTrace();
		}

		// System.out.println(lol.get(0).get(0));
		return lol;

	}

	/**
	 * @author mike
	 * @param data
	 *            this is the data to be split
	 * @return
	 */
	public static List<List<List<String>>> splitListFour(List<List<String>> data) {

		int numColumns = data.size(); // gets number of columns from table
		int numRows = data.get(0).size(); // gets number of rows from table
		List<List<String>> subset1 = new ArrayList<List<String>>(numColumns); // declare list of lists
		List<List<String>> subset2 = new ArrayList<List<String>>(numColumns); // declare list of lists
		List<List<String>> subset3 = new ArrayList<List<String>>(numColumns); // declare list of lists
		List<List<String>> subset4 = new ArrayList<List<String>>(numColumns); // declare list of lists

		// instantiate all columns for subsets
		for (int i = 0; i < numColumns; i++) {
			subset1.add(new ArrayList<String>());
			subset2.add(new ArrayList<String>());
			subset3.add(new ArrayList<String>());
			subset4.add(new ArrayList<String>());
		}

		// 3 Partisans (may cause data loss)
		int bracket1 = (int) (0.25 * numRows);
		int bracket2 = (int) (0.50 * numRows);
		int bracket3 = (int) (0.75 * numRows);

		// spits data into subsets
		for (int row = 0; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++) {
				// if data is in bracket 1 put it in subset 1
				if (row <= bracket1)
					subset1.get(column).add(data.get(column).get(row));
				// if data is in bracket 1 put it in subset 2
				else if (row <= bracket2)
					subset2.get(column).add(data.get(column).get(row));
				// if data is in bracket 1 put it in subset 3
				else if (row <= bracket3)
					subset3.get(column).add(data.get(column).get(row));
				// if data is in bracket 1 put it in subset 4
				else
					subset4.get(column).add(data.get(column).get(row));
			}
		}
		// declare list of subsets
		List<List<List<String>>> allSubsets = new ArrayList<List<List<String>>>(numColumns); // declare list of lists
		// add all subsets
		allSubsets.add(subset1);
		allSubsets.add(subset2);
		allSubsets.add(subset3);
		allSubsets.add(subset4);
		return allSubsets;
	}

}
