package hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceConsumption.ConsumptionEvent;

/**
 * Implementors of this interface should assign the particular resourceconsumptions
 * to each ResourceSpreader in a dependency group
 */
public interface Scheduler {

	/**
	 * The function ensures that each resource consumption is assigned a
	 * processing limit and determines what is the resource consumption which
	 * will finish earliest with that particular limit. The earliest completion
	 * time is then returned to the main resource spreading logic of the
	 * simulator.
	 * 
	 * @param the freqSyncer for which contains the dependency group
	 * @return the earliest completion time
	 */
	public long schedule(ResourceSpreader.FreqSyncer freqSyncer);
	
	/**
	 * Create a ResourceConsumption instance with the given parameters suitable
	 * for scheduling by this scheduler
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
	 *            
	 * @return the ResourceConsumption instance that this scheduler can handle
	 */
	public ResourceConsumption createConsumption(
			final double total, final double limit, final ResourceSpreader consumer,
			final ResourceSpreader provider, final ConsumptionEvent e);
	
	/**
	 * Return a Consumption instance for the implementor, with the state of
	 * the other consumption instance
	 * 
	 * @param other the consumption whose state should be copied
	 */
	public ResourceConsumption createConsumption(ResourceConsumption other);
	
}
