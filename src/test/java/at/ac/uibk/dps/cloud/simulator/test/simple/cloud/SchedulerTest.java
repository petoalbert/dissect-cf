package at.ac.uibk.dps.cloud.simulator.test.simple.cloud;

import org.junit.Before;
import org.junit.Test;

import at.ac.uibk.dps.cloud.simulator.test.ConsumptionEventAssert;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.MaxMinFairScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.NonpreemptiveScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumer;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceProvider;

public class SchedulerTest {

	private ResourceProvider offer;
	private ResourceConsumer utilize;
	private double permsProcessing = 0.001;
	private double processingTaskLen = 1;
	
	@Before
	public void setUp() throws Exception {
		offer = new ResourceProvider(permsProcessing, new MaxMinFairScheduler());
		utilize = new ResourceConsumer(permsProcessing, new MaxMinFairScheduler());
	}

	@Test
	public void testSameScheduler() {
		long expectedTime = Timed.getFireCount() + 1000;
		ResourceConsumption c1 = offer.createConsumption(
				processingTaskLen,
				permsProcessing,
				utilize,
				offer,
				new ConsumptionEventAssert(expectedTime));
		c1.registerConsumption();
		Timed.simulateUntil(expectedTime-1);
		offer.setScheduler(new MaxMinFairScheduler());
		Timed.simulateUntilLastEvent();
	}
	
	@Test
	public void testDifferentScheduler() {
		long expectedTime;
		offer = new ResourceProvider(permsProcessing, new NonpreemptiveScheduler());
		utilize = new ResourceConsumer(permsProcessing, new NonpreemptiveScheduler());
		expectedTime = Timed.getFireCount() + 1000;
		ResourceConsumption c1 = offer.createConsumption(
				processingTaskLen,
				permsProcessing,
				utilize,
				offer,
				new ConsumptionEventAssert(expectedTime));
		c1.registerConsumption();
		Timed.simulateUntil(expectedTime-1);
		offer.setScheduler(new MaxMinFairScheduler());
		Timed.simulateUntilLastEvent();
		
		offer = new ResourceProvider(permsProcessing, new MaxMinFairScheduler());
		utilize = new ResourceConsumer(permsProcessing, new MaxMinFairScheduler());
		expectedTime = Timed.getFireCount() + 1000;
		ResourceConsumption c2 = offer.createConsumption(
				processingTaskLen,
				permsProcessing,
				utilize,
				offer,
				new ConsumptionEventAssert(expectedTime));
		c2.registerConsumption();
		Timed.simulateUntil(expectedTime-1);
		offer.setScheduler(new NonpreemptiveScheduler());
		Timed.simulateUntilLastEvent();
	}

}
