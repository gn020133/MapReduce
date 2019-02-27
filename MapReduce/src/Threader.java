import java.util.ArrayList;
import java.util.List;

/**
 * @author mike this class is used to handle the threads
 */
class Threader implements Runnable {
	static List<List<String>> data = new ArrayList<List<String>>(6); // declare list of lists
	private Thread t1; // the thread
	public DataFunc df; // the data function
	String tName; // thread name
	// constructor

	Threader(String name) {
		tName = name; // sets name of thread
		System.out.println("Constructing " + tName); // tells user the thread is being constructed
	}

	/**
	 * @author mike used to run the thread
	 */
	public void run() {
		try {
			df.preprocess(); // starts pre-processing of data
			Thread.sleep(20); // allows thread to sleep

		} catch (InterruptedException e) {
			System.out.println(tName + " interruptEx.");
		}
		MapReduceAirportMain.finishedThread();
		System.out.println(tName + " finished.");
	}

	/**
	 * @author mike used to start the thread (should be used by the
	 *         MapReduceAirportMain class)
	 * @param data1
	 *            3d array containing subsets of AComp Passenger data
	 * @param data1
	 *            2d array containing lists of top 30 airports
	 */
	public void start(List<List<String>> data1, List<List<String>> data2) {
		// Initialise datafunction
		df = new DataFunc(data1, data2);
		// print starting thread
		System.out.println("Starting " + tName);
		//if start was unsuccessful, try again recursively 
		if (t1 == null) {
			t1 = new Thread(this, tName);
			t1.start();
		}
	}


}