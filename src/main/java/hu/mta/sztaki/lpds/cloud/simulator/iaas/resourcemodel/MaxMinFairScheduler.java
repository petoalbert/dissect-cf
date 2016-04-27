package hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel.ResourceSpreader.FreqSyncer;

/**
 * This class provides scheduling logic based on the max-min fairness algorithm
 */
public class MaxMinFairScheduler implements Scheduler{
	
	/**
	 * This Consumption provides specific functionality needed by this scheduler
	 */
	public static class Consumption extends ResourceConsumption {
		
		/**
		 * The processing limit imposed because of the provider
		 *
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 */
		double providerLimit;
		/**
		 * the processing limit imposed because of the consumer
		 *
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 */
		double consumerLimit;
		/**
		 * the amount of processing that can be surely done by both the provider and
		 * the consumer. This is a temporary variable used by the MaxMinFairSpreader
		 * to determine the provider/consumerLimits.
		 * 
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 */
		double limithelper;
		/**
		 * A helper field to show if the particular resource consumption still
		 * participates in the scheduling process or if it has already finalized its
		 * realLimit value.
		 * 
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 */
		boolean unassigned;
		/**
		 * A helper field to show if the consumer/providerLimit fields are under
		 * update by MaxMinFairSpreader.assignProcessingPower()
		 * 
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 */
		boolean inassginmentprocess;
		/**
		 * The number of ticks it is expected to take that renders both
		 * underProcessing and toBeProcessed as 0 (i.e., the time when the initially
		 * specified amount of resources are completely utilized).
		 */
		private long completionDistance;
		/**
		 * The currently processing entities (e.g., a network buffer)
		 * 
		 * <i>NOTE:</i> as this consumption is generic, it is actually the
		 * provider/consumer pair that determines what is the unit of this field
		 */
		private double underProcessing;


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
		protected Consumption(double total, double limit, ResourceSpreader consumer, ResourceSpreader provider,
				ConsumptionEvent e) {
			super(total, limit, consumer, provider, e);
			underProcessing = 0;
		}
		
		/**
		 * Updates the completion distance field, should be called every time the
		 * real limit is updated or when the amount of unprocessed consumption
		 * changes.
		 */
		protected void calcCompletionDistance() {
			completionDistance = Math.round(getUnProcessed() / realLimit);
		}

		/**
		 * Retrieves the number of ticks it is expected to take that renders both
		 * underProcessing and toBeProcessed as 0 (i.e., the time when the initially
		 * specified amount of resources are completely utilized). This is again
		 * just the value that is derived from the real limit last set by the
		 * scheduler.
		 */
		@Override
		public long getCompletionDistance() {
			return completionDistance;
		}
		
		/**
		 * Sets the real limit based on the scheduler set provider and consumer
		 * limits (the smaller is used as real).
		 * 
		 * Updates the completion distance if instructed.
		 * 
		 * @param updateCD
		 *            <i>true</i> tells the system to update the completion distance
		 *            alongside the real limit setup. This more frequent update on
		 *            the real limit than the completion distance is calculated.
		 * 
		 *            <i>IMPORTANT:</i> if set to false then it is expected that the
		 *            scheduler will call the updateRealLimit at least once more
		 *            with a true parameter. Failing to do so the consumption object
		 *            will become broken.
		 * @return the real limit that was actually determined and set by this
		 *         function
		 * @throws IllegalStateException
		 *             if the real limit would become 0
		 */
		double updateRealLimit(final boolean updateCD) {
			final double rlTrial = providerLimit < consumerLimit ? providerLimit : consumerLimit;
			if (rlTrial == 0) {
				throw new IllegalStateException(
						"Cannot calculate the completion distance for a consumption without a real limit! " + this);
			}
			setRealLimit(rlTrial);
			if (updateCD) {
				calcCompletionDistance();
			}
			return realLimit;
		}
		
		/**
		 * Simulate the processing of this consumption, either from the consumer
		 * side (see doConsumerProcessing) or from the provider side 
		 * (see doProviderProcessing)
		 * 
		 * @param ticksPassed
		 *            the number of ticks to be simulated (i.e. how many times we
		 *            should multiply realLimit) before offering the resources to
		 *            the underprocessing field.
		 * @return the amount of resources actually processed for consumption.
		 *         Negative values mark the end of this resource consumption (i.e.
		 *         when there is no more processing to be done for this
		 *         consumption). Albeit such values are negative, their negativeness
		 *         is just used as a flag and their absolute value still represent
		 *         the amount of offered resources.
		 */
		@Override
		protected double process(long ticksPassed, boolean consumer) {
			if (consumer) {
				return doConsumerProcessing(ticksPassed);
			} else {
				return doProviderProcessing(ticksPassed);
			}
		}
		
		/**
		 * This function simulates how the provider offers the resources for its
		 * consumer. The offered resources are put in the underprocessing field from
		 * the toBeprocessed.
		 * 
		 * If the processing is really close to completion (determined by using
		 * halfreallimit), then this function cheats a bit and offers the resources
		 * for the last remaining processable consumption. This is actually ensuring
		 * that we don't need to simulate sub-tick processing operations.
		 * 
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 * 
		 * @param ticksPassed
		 *            the number of ticks to be simulated (i.e. how many times we
		 *            should multiply realLimit) before offering the resources to
		 *            the underprocessing field.
		 * @return the amount of resources actually offered for consumption.
		 *         Negative values mark the end of this resource consumption (i.e.
		 *         when there is no more processing to be done for this
		 *         consumption). Albeit such values are negative, their negativeness
		 *         is just used as a flag and their absolute value still represent
		 *         the amount of offered resources.
		 */
		double doProviderProcessing(final long ticksPassed) {
			double processed = 0;
			if (toBeProcessed > 0) {
				final double possiblePush = ticksPassed * realLimit;
				processed = possiblePush < toBeProcessed ? possiblePush : toBeProcessed;
				toBeProcessed -= processed;
				underProcessing += processed;
				if (toBeProcessed < halfRealLimit) {
					// ensure that tobeprocessed is 0!
					processed += toBeProcessed;
					underProcessing += toBeProcessed;
					toBeProcessed = 0;
					return -processed;
				}
			}
			return processed;
		}
		
		/**
		 * This function simulates how the consumer utilizes the resources from its
		 * provider. The utilized resources are used from the underprocessing field.
		 * 
		 * If the processing is really close to completion (determined by
		 * calculating the completion distance), then this function cheats a bit and
		 * utilizes the resources for the last remaining underprocessing. This is
		 * actually ensuring that we don't need to simulate sub-tick processing
		 * operations.
		 * 
		 * <i>WARNING:</i> this is necessary for the internal behavior of
		 * MaxMinFairSpreader
		 * 
		 * @param ticksPassed
		 *            the number of ticks to be simulated (i.e. how many times we
		 *            should multiply realLimit) before utilizing the resources from
		 *            the underprocessing field.
		 * @return the amount of resources actually utilized by the consumer.
		 *         Negative values mark the end of this resource consumption (i.e.
		 *         when there is no more processing to be done for this
		 *         consumption). Albeit such values are negative, their negativeness
		 *         is just used as a flag and their absolute value still represent
		 *         the amount of utilized resources.
		 */
		double doConsumerProcessing(final long ticksPassed) {
			if (realLimit == 0) {
				return 0;
			}
			double processed = 0;
			if (underProcessing > 0) {
				final double possibleProcessing = ticksPassed * realLimit;
				processed = possibleProcessing < underProcessing ? possibleProcessing : underProcessing;
				underProcessing -= processed;
				if (Math.round(getUnProcessed()/realLimit) == 0) {
					// ensure that tobeprocessed is 0!
					processed += underProcessing;
					underProcessing = 0;
					return -processed;
				}
			}
			return processed;
		}
		
		/**
		 * Returns the amount of processing still remaining in this resource
		 * consumption.
		 * 
		 * @return the remaining processing
		 */
		@Override
		public double getUnProcessed() {
			return underProcessing + toBeProcessed;
		}
		
		/**
		 * Determines the amount of resoruces already offered by the provider but
		 * not yet used by the consumer.
		 * 
		 * @return the underprocessing value
		 */
		public double getUnderProcessing() {
			return underProcessing;
		}
		
	}
	
	/**
	 * This class contains necessary fields needed by this scheduler. Instances
	 * of this class will be mapped to ResourceSpreader objects. 
	 */
	private static class SchedulingParams {
		private double currentUnProcessed;
		private int unassignedNum;
		private int upLen;
	}
	
	/**
	 * This maps the ResourceSpreader objects to their associated
	 * SchedulingParams object.
	 */
	private Map<ResourceSpreader, SchedulingParams> params;
	
	/**
	 * Create a new MaxMinFairScheduler object, without any ResourceSpreader
	 * object and SchedulingParams.
	 */
	public MaxMinFairScheduler() {
		params = new HashMap<ResourceSpreader, SchedulingParams>();
	}

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
	@Override
	public long schedule(FreqSyncer syncer) {
		// Phase 1: preparation
		final ResourceSpreader[] depgroup = syncer.getDepGroup();
		// Update the parameters
		for (ResourceSpreader spreader : depgroup) {
			if (!params.containsKey(spreader)) {
				params.put(spreader, new SchedulingParams());
			}
			//TODO: removed spreaders should be removed from the map
		}
		final int dglen = syncer.getDGLen();
		final int providerCount = syncer.getFirstConsumerId();
		for (int i = 0; i < dglen; i++) {
			initializeFreqUpdate(depgroup[i]);
		}
		boolean someConsumptionIsStillUnderUtilized;
		// Phase 2: Progressive filling iteration
		do {
			// Phase 2a: determining maximum possible processing
			// Determining wishes for providers and consumers
			for (int i = 0; i < dglen; i++) {
				assignProcessingPower(depgroup[i]);
			}
			// Phase 2b: Finding minimum between providers and consumers
			double minProcessing = Double.MAX_VALUE;
			for (int i = 0; i < providerCount; i++) {
				final int upLen = depgroup[i].underProcessing.size();
				for (int j = 0; j < upLen; j++) {
					final Consumption con = (Consumption)depgroup[i].underProcessing.get(j);
					if (con.unassigned) {
						final double currlimit = con.updateRealLimit(false);
						if (currlimit < minProcessing) {
							minProcessing = currlimit;
						}
					}
				}
			}

			// Phase 2c: single filling
			someConsumptionIsStillUnderUtilized = false;
			for (int i = 0; i < providerCount; i++) {
				ResourceSpreader mmfs = depgroup[i];
				SchedulingParams mmfsParams = params.get(mmfs);
				for (int j = 0; j < mmfsParams.upLen; j++) {
					final Consumption con = (Consumption)mmfs.underProcessing.get(j);
					if (con.unassigned) {
						con.limithelper += minProcessing;
						final ResourceSpreader counterpart = mmfs.getCounterPart(con);
						SchedulingParams counterpartParams = params.get(counterpart);
						mmfsParams.currentUnProcessed -= minProcessing;
						counterpartParams.currentUnProcessed -= minProcessing;
						if (Math.abs(con.getRealLimit() - minProcessing) <= minProcessing * 0.000000001) {
							con.unassigned = false;
							mmfsParams.unassignedNum--;
							counterpartParams.unassignedNum--;
						}
					}
				}
				someConsumptionIsStillUnderUtilized |= mmfsParams.unassignedNum > 0;
			}
		} while (someConsumptionIsStillUnderUtilized);
		// Phase 3: Determining the earliest completion time
		long minCompletionDistance = Long.MAX_VALUE;
		for (int i = 0; i < providerCount; i++) {
			final int upLen = depgroup[i].underProcessing.size();
			for (int j = 0; j < upLen; j++) {
				final Consumption con = (Consumption)depgroup[i].underProcessing.get(j);
				con.consumerLimit = con.providerLimit = con.limithelper;
				con.updateRealLimit(true);
				final long conDistance = con.getCompletionDistance();
				minCompletionDistance = conDistance < minCompletionDistance ? conDistance : minCompletionDistance;
			}
		}
		return minCompletionDistance;
	}
	
	/**
	 * At the beginning of a freq update cycle, every influence group member's
	 * scheduler parameters are initialised with this function.
	 * 
	 * The function assures that the scheduler parameters associated
	 * with the given spreader are initialised, as well as all resource
	 * consumptions in the influence group are set as unassigned and their 
	 * processing limits are set to 0. This step actually allows the max-min 
	 * fairness algorithm to gradually increase the processing limits for 
	 * each resource consumption that could play a role in bottleneck 
	 * situations.
	 * 
	 * @param spreader 
	 *            the ResourceSpreader instance for which we would like to 
	 *            initialize the scheduler state
	 *            
	 * @return <i>true</i> if there were some resource consumptions to be
	 *         processed by this spreader.
	 */
	private boolean initializeFreqUpdate(ResourceSpreader spreader) {
		SchedulingParams p = params.get(spreader);
		p.unassignedNum = p.upLen = spreader.underProcessing.size();
		if (p.unassignedNum == 0) {
			return false;
		}
		for (int i = 0; i < p.upLen; i++) {
			final Consumption con = (Consumption)spreader.underProcessing.get(i);
			con.limithelper = 0;
			con.unassigned = true;
		}
		p.currentUnProcessed = spreader.perTickProcessingPower;
		return true;
	}
	
	/**
	 * Manages the gradual increase of the processing limits for each resource
	 * consumption related to the given spreader. The increase is started from the
	 * limithelper of each resource consumption. This limithelper tells to what
	 * amount the particular consumption was able to already process (i.e. it is
	 * the maximum consumption limit of some of its peers). If a resource
	 * consumption is still unassigned then its limithelper should be still
	 * lower than the maximum amount of processing possible by its
	 * provider/consumer.
	 * 
	 * @param spreader
	 *            the ResourceSpreader instance to assign the processing 
	 *            limits to
	 */
	private void assignProcessingPower(ResourceSpreader spreader) {
		SchedulingParams p = params.get(spreader);
		if (p.currentUnProcessed > spreader.negligableProcessing && p.unassignedNum > 0) {
			int currlen = p.unassignedNum;
			for (int i = 0; i < p.upLen; i++) {
				Consumption con = (Consumption)spreader.underProcessing.get(i);
				con.inassginmentprocess = con.unassigned;
			}
			double currentProcessable = p.currentUnProcessed;
			double pastProcessable;
			int firstindex = 0;
			int lastindex = p.upLen;
			do {
				pastProcessable = currentProcessable;
				final double maxShare = currentProcessable / currlen;
				boolean firstIndexNotSetUp = true;
				int newlastindex = -1;
				for (int i = firstindex; i < lastindex; i++) {
					final Consumption con = (Consumption)spreader.underProcessing.get(i);
					if (con.inassginmentprocess) {
						final double limit = con.getProcessingLimit() - con.limithelper;
						if (limit < maxShare) {
							currentProcessable -= limit;
							updateConsumptionLimit(spreader, con, limit);
							// we move an unprocessed item from the back here
							// then allow reevaluation
							// and also make sure the currlen is reduced
							con.inassginmentprocess = false;
							currlen--;
						} else {
							newlastindex = i;
							if (firstIndexNotSetUp) {
								firstindex = i;
								firstIndexNotSetUp = false;
							}
							updateConsumptionLimit(spreader, con, maxShare);
						}
					}
				}
				lastindex = newlastindex;
			} while (currlen != 0 && pastProcessable != currentProcessable);
		}
	}
	
	/**
	 * Update the consumption limit for the given consumption, in a way that
	 * depends on wheter the spreader is a consumer or a provider
	 * 
	 * @param spreader the spreader to which the given consumption belongs
	 * @param the consumption whose limits should be updated
	 * @param limit set the consumption's limit to this value
	 */
	private void updateConsumptionLimit(
			ResourceSpreader spreader, 
			Consumption con, 
			double limit) {
		
		if (spreader.isConsumer()) {
			con.consumerLimit = limit;
		} else {
			con.providerLimit = limit;
		}
		
	}
	
	/**
	 * Return a Consumption instance that can be used by this scheduler
	 * 
	 * @return a MaxMinFairScheduler.Consumption instance with the specified
	 *         parameters
	 */
	public Consumption createConsumption(
			double total, double limit, ResourceSpreader consumer, ResourceSpreader provider,
			ResourceConsumption.ConsumptionEvent e) {
		return new Consumption(total, limit, consumer, provider, e);
	}

}
