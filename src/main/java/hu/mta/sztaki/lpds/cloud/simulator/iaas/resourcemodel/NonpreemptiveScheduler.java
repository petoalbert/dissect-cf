package hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceSpreader.FreqSyncer;

/**
 * This class is used to scheduler consumptions of providers one by one, i.e.
 * providers will process only one consumption at a time, switching to the 
 * next consumption only when the actual one has been completed, suspended or
 * canceled.
 * 
 * Consumptions are selected for execution based on their registration order.
 * A registered consumption will be processed before those registered after
 * it.
 */
public class NonpreemptiveScheduler implements Scheduler{
	
	/**
	 * This consumptions provides specific fields and functionality for the 
	 * nonpreemptive scheduler.
	 */
	public static class Consumption extends ResourceConsumption {

		/**
		 * This value shows whether this consumption is the current
		 * consumption under processing (true) or not (false)
		 */
		protected boolean underProcessing;
		
		/**
		 * This value contains the amount of processed resources since the 
		 * last scheduling cycle.
		 */
		private double processed;
		
		/**
		 * The "priority" of this consumption. The lower the priority, the 
		 * earlier the consumption will be selected for processing.
		 */
		protected long arrivalId;
		
		/**
		 * This helper field is used to assign increasing arrivalId values
		 * to consumptions registered after each other.
		 */
		private static long lastId = 0;
		
		/**
		 * This constructor describes the basic properties of an individual resource
		 * consumption.
		 * 
		 * @param total
		 *            The amount of processing to be done during the lifetime of the
		 *            just created object
		 * @param limit
		 *            the maximum amount of processing allowable for this particular
		 *            resource consumption (this allows the specification of an
		 *            upper limit of any consumption). If there is no upper limit
		 *            needed then this value should be set with the value of the
		 *            unlimitedProcessing field.
		 * @param consumer
		 *            the consumer that will benefit from the resource consumption.
		 *            This field could be null, then the consumer must be set with
		 *            the setConsumer() function.
		 * @param provider
		 *            the provider which will offer its resources for the consumer.
		 *            This field could be null, then the provider must be set with
		 *            the setProvider() function.
		 * @param e
		 *            Specify here the event to be fired when the just created
		 *            object completes its transfers. With this event it is possible
		 *            to notify the entity who initiated the transfer. This event
		 *            object cannot be null. If there is no special event handling
		 *            is needed then just create a ConsumptionEventAdapter.
		 */
		public Consumption(double total, double limit, ResourceSpreader consumer,
				ResourceSpreader provider, ConsumptionEvent e) {
			super(total, limit, consumer, provider, e);
		}
		
		 /**
		  * Create a consumption using the general consumption fields of the 
		  * other consumption
		  * 
		  * @param other the consumption whose state should be copied
		  */
		 protected Consumption(ResourceConsumption other) {
		 	super(other);
		 }
		
		/**
		 * Return the arrivalId of this consumption
		 * 
		 * @return the value of arrivalId
		 */
		protected long getArrivalId() {
			return arrivalId;
		}	
		
		/**
		 * Actually process resources with this consumption.
		 * 
		 * This method should only be called by the providers. Consumer should
		 * only query the amount of resources processed.
		 * 
		 * @param ticksPassed the time passed since the last scheduling cycle
		 * @return the amount of resources actually processed for consumption.
		 *         Negative values mark the end of this resource consumption (i.e.
		 *         when there is no more processing to be done for this
		 *         consumption). Albeit such values are negative, their negativeness
		 *         is just used as a flag and their absolute value still represent
		 *         the amount of offered resources.
		 */
		public double process(final long ticksPassed, boolean consumer) {
			if (consumer) {
				return processed;
			}
			processed = 0;
			if (realLimit > 0) {
				final double possibleProcessing = ticksPassed * realLimit;
				processed = possibleProcessing < toBeProcessed ? possibleProcessing : toBeProcessed;
				toBeProcessed -= processed;
				if (Math.round(getUnProcessed()/realLimit) == 0) {
					// ensure that tobeprocessed is 0!
					processed += getUnProcessed();
					processed = -processed;
					return processed;
				}
			}
			return processed;
		}
		
		/**
		 * Initiates the processing of a resource consumption. By calling this
		 * function the resource consumption object will be participating in the
		 * unified resource sharing mechanism's scheduling and spreading operations.
		 * 
		 * Before the registration actually happens, it updates the hard limit of
		 * the consumption so the spreaders could now operate using its values.
		 * 
		 * <i>NOTE:</i> this function is also used for resuming a suspended
		 * consumption
		 * 
		 * @return
		 * 		<ul>
		 *         <li><i>true</i> if the registration was successful
		 *         <li><i>false</i> otherwise. For example: if the provider/consumer
		 *         is not yet set, if the consumption cannot be registered between
		 *         the particular provider/consumer pair or if the consumption was
		 *         already registered.
		 *         </ul>
		 */
		@Override
		public boolean registerConsumption() {
			boolean success = super.registerConsumption();
			if (success) {
				this.arrivalId = lastId++;
			}
			return success;
		}
		
		/**
		 * Get the amount of processed resources since the last scheduling cycle
		 * 
		 * @return the amount of resources actually processed for consumption.
		 *         Negative values mark the end of this resource consumption (i.e.
		 *         when there is no more processing to be done for this
		 *         consumption). Albeit such values are negative, their negativeness
		 *         is just used as a flag and their absolute value still represent
		 *         the amount of offered resources.
		 */
		public double getProcessed() {
			return processed;
		}

		/**
		 * Retrieves the number of ticks it is expected to take that renders both
		 * underProcessing and toBeProcessed as 0 (i.e., the time when the initially
		 * specified amount of resources are completely utilized). This is again
		 * just the value that is derived from the real limit last set by the
		 * scheduler.
		 */
		public long getCompletionDistance() {
			if (underProcessing) {
				return Math.round(getUnProcessed() / realLimit);
			} else {
				throw new IllegalStateException("I am in illegal state");
			}
		}
		
		/**
		 * Terminates the consumption but ensures that it will be resumable later
		 * on. If a consumption needs to be resumed then it must be re-registered,
		 * there is no special function for resume.
		 */
		@Override
		public void suspend() {
			super.suspend();
			underProcessing = false;
		}
	}
	
	/**
	 * The collection of the Consumers that have consumptions scheduler for
	 * processing by a nonpreemptive scheduler.
	 * 
	 * This field is needed for proper scheduling when dependency groups are
	 * merged together, and there are consumptions from the "new" group 
	 * for which the providers of the "group of this scheduler" would assign 
	 * consumptions, even though they had active consumptions in the previous
	 * group.
	 */
	private Collection<ResourceSpreader> underProcessing;
	
	/**
	 * Create a nonpreemptive scheduler instance.
	 */
	public NonpreemptiveScheduler() {
		underProcessing = new HashSet<ResourceSpreader>();
	}

	/**
	 * The function assigns one consumption for each provider to execute,
	 * if possible. Other consumptions real limits are set to zero.
	 * 
	 * @param the freqSyncer for which contains the dependency group
	 * @return the earliest completion time
	 */
	@Override
	public long schedule(FreqSyncer freqSyncer) {
		mergeSchedulerStates(freqSyncer);
		long earliestCompletion = Long.MAX_VALUE;
		ResourceSpreader[] spreaders = freqSyncer.getDepGroup();
		for (int i=0; i<freqSyncer.getFirstConsumerId(); i++) {
			Consumption c = findEarliestConsumption(spreaders[i]);
			if (c == null) {
				continue;
			}
			if (c.getCompletionDistance() < earliestCompletion) {
				earliestCompletion = c.getCompletionDistance();
			}
			for (ResourceConsumption consumption : spreaders[i].underProcessing) {
				if (consumption != c) {
					consumption.setRealLimit(0);
				}
			}
		}
		return earliestCompletion;
	}
	
	/**
	 * Remove those consumers from the underProcessing field that have finished
	 * processing their assigned consumption, so that they can be assigned 
	 * new consumptions.
	 */
	private void updateUnderProcessing() {
		List<ResourceSpreader> toRemove = new LinkedList<ResourceSpreader>();
		for (ResourceSpreader s : underProcessing) {
			boolean isProcessing = false;
			for (ResourceConsumption c: s.underProcessing) {
				if (((Consumption)c).underProcessing) {
					isProcessing = true;
					break;
				}
			}
			if (!isProcessing) {
				toRemove.add(s);
			}
		}
		underProcessing.removeAll(toRemove);
	}
	
	/**
	 * Merge the scheduler states from dependency groups that were recently
	 * merged with this dependency group
	 * 
	 * @param freqSyncer the syncer of the dependency group
	 */
	private void mergeSchedulerStates(FreqSyncer freqSyncer) {
		for (ResourceSpreader s : freqSyncer.getDepGroup()) {
			if (s == null) {
				continue;
			}
			if (s.scheduler == null) {
				System.out.println("Nagy baj");
			}
			if (((NonpreemptiveScheduler)s.scheduler).underProcessing == null) {
				System.out.println("Oh oh");
			}
			underProcessing.addAll(((NonpreemptiveScheduler)s.scheduler).underProcessing);
		}
		updateUnderProcessing();
	}
	
	/**
	 * Return the Consumption with the earliest arrival time (registered before
	 * the others) that can be scheduled for processing, or null if there is 
	 * no consumption that could be processed.
	 * 
	 * @param spreader the provider for which we should find the consumption
	 * @return the consumption with the earliest arrivalTime that can be 
	 *         processed
	 */
	private Consumption findEarliestConsumption(ResourceSpreader spreader) {
		long arrivalTime = Long.MAX_VALUE;
		Consumption c = null;
		for (ResourceConsumption consumption : spreader.underProcessing) {
			Consumption nc = (Consumption)consumption;
			if (nc.getArrivalId() < arrivalTime) {			
				arrivalTime = nc.getArrivalId();
				if (nc.underProcessing) {
					c = nc;
					break;
				}
				if (underProcessing.contains(nc.getConsumer())) {
					continue;
				}
				c = nc;
			}
		}
		
		if (c != null) {
			c.underProcessing = true;
			c.setRealLimit(c.getHardLimit());
			underProcessing.add(c.getConsumer());
		}
		return c;
	}

	/**
	 * Return a Consumption instance that can be used by this scheduler
	 * 
	 * @return a NonpreemptiveScheduler.Consumption instance with the specified
	 *         parameters
	 */
	@Override
	public ResourceConsumption createConsumption(double total, double limit, ResourceSpreader consumer,
			ResourceSpreader provider, ConsumptionEvent e) {
		return new Consumption(total, limit, consumer, provider, e);
	}
	
	/**
	 * Return a Consumption instance for this scheduler, with the state of
	 * the other consumption instance
	 * 
	 * @param other the consumption whose state should be copied
	 */
	public Consumption createConsumption(ResourceConsumption other) {
		if (!(other instanceof Consumption)) {
			return new Consumption(other);
		} else {
			return (Consumption)other;
		}
	}
}
