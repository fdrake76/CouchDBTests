import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: fdrake
 * Date: 11/12/11
 * Time: 7:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class PerformanceMetric {
    private List<Long> measurements;

    public PerformanceMetric() {
        measurements = new ArrayList<Long>();
    }

    public void addMeasurement(long measurement) {
        measurements.add(measurement);
    }

    public long getMaxMeasurement() {
        if (measurements.size() == 0) return 0;
        long max = measurements.get(0);
        for(int i=1;i<measurements.size();i++) {
            if (measurements.get(i) > max) max = measurements.get(i);
        }
        return max;
    }

    public long getMinMeasurement() {
        if (measurements.size() == 0) return 0;
        long min = measurements.get(0);
        for(int i=1;i<measurements.size();i++) {
            if (measurements.get(i) < min) min = measurements.get(i);
        }
        return min;
    }

    public long getTotalTime() {
        long runningTotal = 0;
        for(long measurement : measurements) { runningTotal += measurement; }
        return runningTotal;
    }

    public double getMeanMeasurement() {
        if (measurements.size() == 0) return 0;
        return (double)getTotalTime() / (double)measurements.size();
    }

    public double getMedianMeasurement() {
        List<Long> m = new ArrayList<Long>();
        Collections.copy(m, measurements);
        Collections.sort(m);

        if (m.size() %2 == 1)
            return m.get((m.size()+1)/2-1);
        else {
            double lower = m.get(m.size()/2-1);
            double upper = m.get(m.size()/2);
            return (lower + upper) / 2.0;
        }
    }
}
