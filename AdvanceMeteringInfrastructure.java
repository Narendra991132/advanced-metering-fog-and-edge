package org.fog.test.perfeval;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.power.PowerHost;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.Application;
import org.fog.application.selectivity.FractionalSelectivity;
import org.fog.entities.*;
import org.fog.placement.Controller;
import org.fog.placement.ModuleMapping;
import org.fog.placement.ModulePlacementEdgewards;
import org.fog.placement.ModulePlacementMapping;
import org.fog.policy.AppModuleAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.utils.FogLinearPowerModel;
import org.fog.utils.FogUtils;
import org.fog.utils.TimeKeeper;
import org.fog.utils.distribution.DeterministicDistribution;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

/**
 * Simulation for Advance Metering Infrastructure
 *
 * @author Narendra : x23429615
 */
public class AdvanceMeteringInfrastructure {
    static List<FogDevice> fogDevices = new ArrayList<FogDevice>();
    static List<Sensor> sensors = new ArrayList<Sensor>();
    static List<Actuator> actuators = new ArrayList<Actuator>();
    static int numOfSites = 30;
    static int numOfPulseMeterPerField = 8;
    static int numOfElectricitySensors = 10;
    static int frequencySensorPerFields = 4;
    static int analogValueMeterPerField = 6;

    private static boolean CLOUD = false;

    public static void main(String[] args) {

        Log.printLine("Starting Advance Metering Infrastructure Edge Simulation");

        try {
            Log.disable();
            int num_user = 1; // number of cloud users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false; // mean trace events

            CloudSim.init(num_user, calendar, trace_flag);

            String appId = "PrecisionAgricultureQualityControlApplication"; // identifier of the application

            FogBroker broker = new FogBroker("broker");

            Application application = createApplication(appId, broker.getId());
            application.setUserId(broker.getId());

            createFogDevices(broker.getId(), appId);

            Controller controller = null;

            ModuleMapping moduleMapping = ModuleMapping.createModuleMapping(); // initializing a module mapping
            for (FogDevice device : fogDevices) {

                if (device.getName().startsWith("m")) { // names of all Pulse devices start with 'm'
                    moduleMapping.addModuleToDevice("pulse-monitoring", device.getName());  // fixing 1 instance of the pulse monitoring module to each Pulse device
                }

                if (device.getName().startsWith("a")) { // names of all ElectricFrequency devices start with 'a'
                    moduleMapping.addModuleToDevice("electric-frequency-sensing", device.getName());  // fixing 1 instance of the frequencySensor sensing module
                }

                if (device.getName().startsWith("b")) { // names of all HUMIDITY Sensor devices start with 'b'
                    moduleMapping.addModuleToDevice("electricity-usage", device.getName());  // fixing 1 instance of the motion detection module
                }

                if (device.getName().startsWith("p")) { // names of all Water Sensor devices start with 'p'
                    moduleMapping.addModuleToDevice("water-sensing", device.getName());  // fixing 1 instance of the water sensing module
                }

                if (device.getName().startsWith("f")) { // names of all AnalogValueMeter devices start with 'f'
                    moduleMapping.addModuleToDevice("analog-value-usage", device.getName());  // fixing 1 instance of the impact detection module
                }

            }

            moduleMapping.addModuleToDevice("smart-meter-analytics", "cloud"); // fixing instances of quality assessment module in the Cloud
            if (CLOUD) {
                // if the mode of deployment is cloud-based
                moduleMapping.addModuleToDevice("analog-value-usage", "cloud");
                moduleMapping.addModuleToDevice("electricity-usage", "cloud");
                moduleMapping.addModuleToDevice("pulse-monitoring", "cloud");
                moduleMapping.addModuleToDevice("electric-frequency-sensing", "cloud");
                moduleMapping.addModuleToDevice("smart-meter-analytics", "cloud");
                moduleMapping.addModuleToDevice("water-sensing", "cloud");
            }

            controller = new Controller("master-controller", fogDevices, sensors,
                    actuators);

            controller.submitApplication(application,
                    (CLOUD) ? (new ModulePlacementMapping(fogDevices, application, moduleMapping))
                            : (new ModulePlacementEdgewards(fogDevices, sensors, actuators, application, moduleMapping)));

            TimeKeeper.getInstance().setSimulationStartTime(Calendar.getInstance().getTimeInMillis());

            CloudSim.startSimulation();

            CloudSim.stopSimulation();

            Log.printLine("Precision Agriculture Quality Control Simulation Finished!");
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    /**
     * Creates the fog devices in the physical topology of the simulation.
     *
     * @param userId
     * @param appId
     */
    private static void createFogDevices(int userId, String appId) {
        FogDevice cloud = createFogDevice("cloud", 44800, 40000, 100, 10000, 0, 0.01, 16 * 103, 16 * 83.25);
        cloud.setParentId(-1);
        fogDevices.add(cloud);
        FogDevice proxy = createFogDevice("proxy-server", 2800, 4000, 10000, 10000, 1, 0.0, 107.339, 83.4333);
        proxy.setParentId(cloud.getId());
        proxy.setUplinkLatency(100); // latency of connection between proxy server and cloud is 100 ms
        fogDevices.add(proxy);
        for (int i = 0; i < numOfSites; i++) {
            addArea(i + "", userId, appId, proxy.getId());
        }
    }

    private static FogDevice addArea(String id, int userId, String appId, int parentId) {
        FogDevice router = createFogDevice("d-" + id, 2800, 4000, 10000, 10000, 2, 0.0, 107.339, 83.4333);
        fogDevices.add(router);
        router.setUplinkLatency(2); // latency of connection between router and proxy server is 2 ms
        for (int i = 0; i < numOfPulseMeterPerField; i++) {
            String mobileId = id + "-" + i;
            FogDevice pulse = addPulse(mobileId, userId, appId, router.getId()); // adding a pulse to the physical topology
            pulse.setUplinkLatency(2); // latency of connection between pulse and router is 2 ms
            fogDevices.add(pulse);
        }

        FogDevice motionDetection = addElectricityFogDevice(id, router.getId());
        motionDetection.setUplinkLatency(2); // latency of connection between HUMIDITY sensor and router is 2 ms
        motionDetection.setParentId(router.getId());
        for (int i = 0; i < numOfElectricitySensors; i++) {
            String mobileId = id + "-" + i;
            addElectricitySensor(mobileId, userId, appId, router.getId(), motionDetection); // adding a HUMIDITY sensor to the physical topology
        }
        fogDevices.add(motionDetection);

        Actuator alert = new Actuator("halm-" + id, userId, appId, "ELECTRICITY-MONITOR");
        actuators.add(alert);
        alert.setGatewayDeviceId(motionDetection.getId());
        alert.setLatency(1.0);

        FogDevice analogValueMeter = addAnalogValueMeter(id, userId, appId, router.getId()); // adding an analogValueMeter to the physical topology
        analogValueMeter.setUplinkLatency(2); // latency of connection between analogValueMeter and router is 2 ms
        fogDevices.add(analogValueMeter);

        FogDevice frequencySensorSensor = addElectricFrequencySensor(id, userId, appId, router.getId()); // adding a frequencySensor sensor to the physical topology
        frequencySensorSensor.setUplinkLatency(2); // latency of connection between frequencySensor sensor and router is 2 ms
        fogDevices.add(frequencySensorSensor);

        FogDevice waterSensor = addWaterSensor(id, userId, appId, router.getId()); // adding a water sensor to the physical topology
        waterSensor.setUplinkLatency(2); // latency of connection between water sensor and router is 2 ms
        fogDevices.add(waterSensor);

        router.setParentId(parentId);
        return router;
    }

    private static FogDevice addPulse(String id, int userId, String appId, int parentId) {
        FogDevice pulse = createFogDevice("m-" + id, 500, 1000, 10000, 10000, 3, 0, 87.53, 82.44);
        pulse.setParentId(parentId);
        Sensor sensor = new Sensor("s-" + id, "PULSE", userId, appId, new DeterministicDistribution(5)); // inter-transmission time of pulse (sensor) follows a deterministic distribution
        sensors.add(sensor);
        Actuator pulseMonitor = new Actuator("pul-" + id, userId, appId, "PULSE_MONITOR");
        actuators.add(pulseMonitor);
        sensor.setGatewayDeviceId(pulse.getId());
        sensor.setLatency(1.0);  // latency of connection between pulse (sensor) and the parent device is 1 ms
        pulseMonitor.setGatewayDeviceId(pulse.getId());
        pulseMonitor.setLatency(1.0);  // latency of connection between pulse actuator and the parent device is 1 ms
        return pulse;
    }

    private static FogDevice addAnalogValueMeter(String id, int userId, String appId, int parentId) {
        FogDevice analogValueMeter = createFogDevice("f-" + id, 2800, 4000, 10000, 10000, 3, 0.0, 107.339, 83.4333);
        analogValueMeter.setParentId(parentId);
        Sensor sensor;
        for (int i = 0; i < analogValueMeterPerField; i++) {
            sensor = new Sensor("fs-" + id, "ANALOG-VALUE", userId, appId, new DeterministicDistribution(5)); // inter-transmission time follows a deterministic distribution
            sensor.setGatewayDeviceId(analogValueMeter.getId());
            sensor.setLatency(1.0);  // latency of connection between analogValueMeter (sensor) and the parent device is 1 ms
            sensors.add(sensor);
        }

        Actuator impactActuator = new Actuator("imp-" + id, userId, appId, "ANALOG_VALUE_MONITOR");
        actuators.add(impactActuator);
        impactActuator.setGatewayDeviceId(analogValueMeter.getId());
        impactActuator.setLatency(1.0);  // latency of connection between impact alert actuator and the parent device is 1 ms
        return analogValueMeter;
    }

    private static FogDevice addElectricFrequencySensor(String id, int userId, String appId, int parentId) {
        FogDevice frequencySensorDevice = createFogDevice("a-" + id, 2800, 4000, 10000, 10000, 3, 0.0, 107.339, 83.4333);
        frequencySensorDevice.setParentId(parentId);
        Sensor sensor;
        for (int i = 0; i < frequencySensorPerFields; i++) {
            sensor = new Sensor("as-" + id, "FREQUENCY", userId, appId, new DeterministicDistribution(5)); // inter-transmission time follows a deterministic distribution
            sensor.setGatewayDeviceId(frequencySensorDevice.getId());
            sensor.setLatency(1.0);  // latency of connection between frequencySensor sensor and the parent device is 1 ms
            sensors.add(sensor);
        }

        Actuator frequencySensorActuator = new Actuator("prx-" + id, userId, appId, "FREQUENCY_MONITOR");
        actuators.add(frequencySensorActuator);
        frequencySensorActuator.setGatewayDeviceId(frequencySensorDevice.getId());
        frequencySensorActuator.setLatency(1.0);  // latency of connection between frequencySensor control actuator and the parent device is 1 ms
        return frequencySensorDevice;
    }

    private static FogDevice addWaterSensor(String id, int userId, String appId, int parentId) {
        FogDevice waterDevice = createFogDevice("p-" + id, 2800, 4000, 10000, 10000, 2, 0.0, 107.339, 83.4333);
        waterDevice.setParentId(parentId);
        Sensor sensor = new Sensor("ps-" + id, "WATER", userId, appId, new DeterministicDistribution(5)); // inter-transmission time follows a deterministic distribution
        sensors.add(sensor);
        Actuator waterActuator = new Actuator("tmp-" + id, userId, appId, "WATER_MONITOR");
        actuators.add(waterActuator);
        sensor.setGatewayDeviceId(waterDevice.getId());
        sensor.setLatency(1.0);  // latency of connection between water sensor and the parent device is 1 ms
        waterActuator.setGatewayDeviceId(waterDevice.getId());
        waterActuator.setLatency(1.0);  // latency of connection between water control actuator and the parent device is 1 ms
        return waterDevice;
    }

    private static FogDevice addElectricityFogDevice(String id, int parentId) {
        FogDevice electricitySensorDevice = createFogDevice("b-" + id, 2800, 4000, 10000, 10000, 3, 0.0, 107.339, 83.4333);
        electricitySensorDevice.setParentId(parentId);
        return electricitySensorDevice;
    }

    private static void addElectricitySensor(String id, int userId, String appId, int parentId, FogDevice electricitySensorDevice) {
        Sensor sensor = new Sensor("bs-" + id, "ELECTRICITY", userId, appId, new DeterministicDistribution(5)); // inter-transmission time follows a deterministic distribution
        sensors.add(sensor);
        sensor.setGatewayDeviceId(electricitySensorDevice.getId());
        sensor.setLatency(1.0);  // latency of connection between HUMIDITY sensor and the parent device is 1 ms
    }

    /**
     * Creates a vanilla fog device
     *
     * @param nodeName    name of the device to be used in simulation
     * @param mips        MIPS
     * @param ram         RAM
     * @param upBw        uplink bandwidth
     * @param downBw      downlink bandwidth
     * @param level       hierarchy level of the device
     * @param ratePerMips cost rate per MIPS used
     * @param busyPower
     * @param idlePower
     * @return
     */
    private static FogDevice createFogDevice(String nodeName, long mips,
                                             int ram, long upBw, long downBw, int level, double ratePerMips, double busyPower, double idlePower) {

        List<Pe> peList = new ArrayList<Pe>();

        // 3. Create PEs and add these into a list.
        peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

        int hostId = FogUtils.generateEntityId();
        long storage = 1000000; // host storage
        int bw = 10000;

        PowerHost host = new PowerHost(
                hostId,
                new RamProvisionerSimple(ram),
                new BwProvisionerOverbooking(bw),
                storage,
                peList,
                new StreamOperatorScheduler(peList),
                new FogLinearPowerModel(busyPower, idlePower)
        );

        List<Host> hostList = new ArrayList<Host>();
        hostList.add(host);

        String arch = "x86"; // system architecture
        String os = "Linux"; // operating system
        String vmm = "Xen";
        double time_zone = 10.0; // time zone this resource located
        double cost = 3.0; // the cost of using processing in this resource
        double costPerMem = 0.05; // the cost of using memory in this resource
        double costPerStorage = 0.001; // the cost of using storage in this
        // resource
        double costPerBw = 0.0; // the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
        // devices by now

        FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
                arch, os, vmm, host, time_zone, cost, costPerMem,
                costPerStorage, costPerBw);

        FogDevice fogdevice = null;
        try {
            fogdevice = new FogDevice(nodeName, characteristics,
                    new AppModuleAllocationPolicy(hostList), storageList, 10, upBw, downBw, 0, ratePerMips);
        } catch (Exception e) {
            e.printStackTrace();
        }

        fogdevice.setLevel(level);
        return fogdevice;
    }

    /**
     * Function to create the Precision Agriculture Quality Control application in the DDF model.
     *
     * @param appId  unique identifier of the application
     * @param userId identifier of the user of the application
     * @return
     */
    @SuppressWarnings({"serial"})
    private static Application createApplication(String appId, int userId) {

        Application application = Application.createApplication(appId, userId);
        /*
         * Adding modules (vertices) to the application model (directed graph)
         */
        application.addAppModule("analog-value-usage", 10);
        application.addAppModule("electric-frequency-sensing", 10);
        application.addAppModule("water-sensing", 10);
        application.addAppModule("electricity-usage", 10);
        application.addAppModule("pulse-monitoring", 10);
        application.addAppModule("smart-meter-analytics", 10);

        /*
         * Connecting the application modules (vertices) in the application model (directed graph) with edges
         */
        application.addAppEdge("PULSE", "pulse-monitoring", 1000, 20000, "PULSE", Tuple.UP, AppEdge.SENSOR);
        application.addAppEdge("pulse-monitoring", "PULSE_MONITOR", 1000, 20000, "PULSE_DATA", Tuple.UP, AppEdge.ACTUATOR);

        application.addAppEdge("ELECTRICITY", "electricity-usage", 500, 2000, "ELECTRICITY", Tuple.UP, AppEdge.SENSOR);
        application.addAppEdge("electricity-usage", "ELECTRICITY-MONITOR", 100, 28, 100, "ELECTRICITY_DATA", Tuple.DOWN, AppEdge.ACTUATOR);

        application.addAppEdge("FREQUENCY", "electric-frequency-sensing", 2000, 2000, "FREQUENCY", Tuple.UP, AppEdge.SENSOR);
        application.addAppEdge("electric-frequency-sensing", "FREQUENCY_MONITOR", 100, 28, 100, "FREQUENCY_DATA", Tuple.DOWN, AppEdge.ACTUATOR);

        application.addAppEdge("WATER", "water-sensing", 2000, 2000, "WATER", Tuple.UP, AppEdge.SENSOR);
        application.addAppEdge("water-sensing", "WATER_MONITOR", 100, 50, 100, "WATER_DATA", Tuple.DOWN, AppEdge.ACTUATOR);

        application.addAppEdge("ANALOG-VALUE", "analog-value-usage", 2000, 2000, "ANALOG-VALUE", Tuple.UP, AppEdge.SENSOR);
        application.addAppEdge("analog-value-usage", "ANALOG_VALUE_MONITOR", 100, 28, 100, "ANALOG_VALUE_DATA", Tuple.DOWN, AppEdge.ACTUATOR);

        /*
         * Defining the input-output relationships (represented by selectivity) of the application modules.
         */
        application.addTupleMapping("pulse-monitoring", "PULSE", "PULSE_DATA", new FractionalSelectivity(1.0));
        application.addTupleMapping("electricity-usage", "ELECTRICITY", "ELECTRICITY_DATA", new FractionalSelectivity(1.0));
        application.addTupleMapping("water-sensing", "WATER", "WATER_DATA", new FractionalSelectivity(1.0));
        application.addTupleMapping("electric-frequency-sensing", "FREQUENCY", "FREQUENCY_DATA", new FractionalSelectivity(0.05));
        application.addTupleMapping("analog-value-usage", "ANALOG-VALUE", "ANALOG_VALUE_DATA", new FractionalSelectivity(0.05));
        application.addTupleMapping("smart-meter-analytics", "ELECTRICITY_DATA", "QUALITY_REPORT", new FractionalSelectivity(1.0));

        /*
         * Defining application loops to monitor the latency.
         */
        final AppLoop loop2 = new AppLoop(new ArrayList<String>() {{
            add("ELECTRICITY");
            add("electricity-usage");
            add("ELECTRICITY-MONITOR");
        }});
        final AppLoop loop3 = new AppLoop(new ArrayList<String>() {{
            add("FREQUENCY");
            add("electric-frequency-sensing");
            add("FREQUENCY_MONITOR");
        }});
        final AppLoop loop4 = new AppLoop(new ArrayList<String>() {{
            add("WATER");
            add("water-sensing");
            add("WATER_MONITOR");
        }});
        final AppLoop loop5 = new AppLoop(new ArrayList<String>() {{
            add("ANALOG-VALUE");
            add("analog-value-usage");
            add("ANALOG_VALUE_MONITOR");
        }});
        final AppLoop loop6 = new AppLoop(new ArrayList<String>() {{
            add("PULSE");
            add("pulse-monitoring");
            add("PULSE_MONITOR");
        }});

        List<AppLoop> loops = new ArrayList<AppLoop>() {
            {
                add(loop2);
                add(loop3);
                add(loop4);
                add(loop5);
                add(loop6);
            }
        };

        application.setLoops(loops);
        return application;
    }
}