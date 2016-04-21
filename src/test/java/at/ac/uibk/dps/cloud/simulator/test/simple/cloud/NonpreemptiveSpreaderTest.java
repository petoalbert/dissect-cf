package at.ac.uibk.dps.cloud.simulator.test.simple.cloud;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import at.ac.uibk.dps.cloud.simulator.test.ConsumptionEventAssert;
import at.ac.uibk.dps.cloud.simulator.test.ConsumptionEventFoundation;
import hu.mta.sztaki.lpds.cloud.simulator.Timed;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.NonpreemptiveScheduler;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumer;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceProvider;

public class NonpreemptiveSpreaderTest extends ConsumptionEventFoundation {
	
	private ResourceProvider offer;
	private ResourceConsumer utilize;
	private double permsProcessing = 0.001;
	private double processingTaskLen = 1;
	
	@Before
	public void setUp() throws Exception {
		offer = new ResourceProvider(ResourceConsumptionTest.permsProcessing, new NonpreemptiveScheduler());
		utilize = new ResourceConsumer(ResourceConsumptionTest.permsProcessing, new NonpreemptiveScheduler());
	}

	@Test
	public void testSimpleConsumption() {
		long expectedFinish = Math.round(2*processingTaskLen/permsProcessing);
		ResourceConsumption c = offer.createConsumption(
						processingTaskLen, 
						permsProcessing/2, 
						utilize, 
						offer, 
						new ConsumptionEventAssert(expectedFinish));
		c.registerConsumption();

		// Test also what happens when we suspend a consumption and continue after
		
		expectedFinish = expectedFinish + Math.round(processingTaskLen/permsProcessing);
		c = offer.createConsumption(
						processingTaskLen, 
						permsProcessing, 
						utilize, 
						offer, 
						new ConsumptionEventAssert(expectedFinish));
		c.registerConsumption();
		
		Timed.simulateUntilLastEvent();
	}
	
	@Test
	public void testMultipleConsumptions() {
		ResourceProvider offer2 =  new ResourceProvider(
				ResourceConsumptionTest.permsProcessing, 
				new NonpreemptiveScheduler());
		ResourceConsumer utilize2 = new ResourceConsumer(
				ResourceConsumptionTest.permsProcessing, 
				new NonpreemptiveScheduler());
		
		long expectedFinish1 = Math.round(processingTaskLen/permsProcessing);
		ResourceConsumption c = offer.createConsumption(
						processingTaskLen, 
						permsProcessing, 
						utilize, 
						offer, 
						new ConsumptionEventAssert(expectedFinish1));
		c.registerConsumption();
		
		long expectedFinish2 = Math.round(2*processingTaskLen/permsProcessing);
		c = offer2.createConsumption(
						processingTaskLen, 
						permsProcessing/2, 
						utilize2, 
						offer2, 
						new ConsumptionEventAssert(expectedFinish2));
		c.registerConsumption();
		
		Timed.simulateUntil(expectedFinish1/2);
		
		long expectedFinish3 = expectedFinish2 + Math.round(processingTaskLen/permsProcessing);
		ConsumptionEventAssert ca = new ConsumptionEventAssert(expectedFinish3);
		c = offer2.createConsumption(
						processingTaskLen, 
						permsProcessing, 
						utilize2, 
						offer, 
						ca);
		c.registerConsumption();
		
		Timed.simulateUntilLastEvent();
		Assert.assertTrue("Completed", ca.isCompleted());		
	}

}
