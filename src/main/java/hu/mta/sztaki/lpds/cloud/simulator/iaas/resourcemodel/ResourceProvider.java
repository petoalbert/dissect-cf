/*
 *  ========================================================================
 *  DIScrete event baSed Energy Consumption simulaTor 
 *    					             for Clouds and Federations (DISSECT-CF)
 *  ========================================================================
 *  
 *  This file is part of DISSECT-CF.
 *  
 *  DISSECT-CF is free software: you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *  
 *  DISSECT-CF is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
 *  General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with DISSECT-CF.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  (C) Copyright 2014, Gabor Kecskemeti (gkecskem@dps.uibk.ac.at,
 *   									  kecskemeti.gabor@sztaki.mta.hu)
 */

package hu.mta.sztaki.lpds.cloud.simulator.iaas.resourcemodel;

/**
 * This class serves as a provider wrapper for the more generic ResourceSpreader.
 *
 * Instances of this class have the same behaviour and characteristics as
 * the ResourceSpreader, except they are always providers.
 */
public class ResourceProvider extends ResourceSpreader {
	
	/**
	 * Constructs a ResourceProvider with the given capacity and scheduler
	 * 
	 * @param initialProcessing
	 *            determines the amount of resources this provider could utilize
	 *            in a single tick
	 * @param scheduler 
	 * 			  the scheduler used to schedule this provider
	 */
	public ResourceProvider(final double initialProcessing, Scheduler scheduler) {
		super(initialProcessing, false, scheduler);
	}

	/**
	 * provides some textual representation of this provider, good for debugging
	 * and tracing
	 */
	@Override
	public String toString() {
		return "ResourceProvider(Hash-" + hashCode() + " " + super.toString() + ")";
	}

}
