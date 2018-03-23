/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.camunda.bpm.engine.impl.processengine;

import org.camunda.bpm.engine.ProcessEngineConfiguration;

/**
 * <p>Determines the amount of time for which the Process Engine bootstrap
 * will have to wait until it attempts to perform reconfiguration commands</p>
 *
 * <p>The time is calculated by using exponential back off using levels.
 * The initial level starts at 0 and increases with the consecutive calls to
 * the calculateWaitTime() method.</p>
 *
 * <p>The wait time is calculated using:</p>
 *
 * <pre>timeToWait = baseBackOffTime * (backOffFactor ^ (backOffLevel - 1))</pre>
 *
 * <p>While the maximum possible back off level is:</p>
 *
 * <pre>maximumLevel = floor( log( backOffFactor, maxBackOffTime / baseBackOffTime) ) + 1</pre>
 *
 * <p>(where log(a, b) is the logarithm of b to the base of a)</p>
 *
 * <p>See #{link {@link org.camunda.bpm.engine.impl.jobexecutor.BackoffJobAcquisitionStrategy}}
 * for a more advanced implementation of a Back off timer</p>
 *
 * @author Nikola Koevski
 */
public class BackOffProcessEngineStrategy implements ProcessEngineStrategy {

  /*
   * BackOff parameters
   */
  protected long baseBackOffTime;
  protected float backOffFactor;
  protected int backOffLevel;
  protected int maxBackOffLevel;
  protected long maxBackOffTime;
  protected boolean applyJitter = false;

  public BackOffProcessEngineStrategy(long baseBackOffTime, float backOffFactor, long maxBackOffTime, boolean applyJitter) {
    this.baseBackOffTime = baseBackOffTime;
    this.backOffFactor = backOffFactor;
    this.maxBackOffTime = maxBackOffTime;
    this.applyJitter = applyJitter;

    initMaxBackOffLevel();
  }

  public BackOffProcessEngineStrategy(ProcessEngineConfiguration processEngineConfiguration) {
    this(
      processEngineConfiguration.getBaseBackOffTime(),
      processEngineConfiguration.getBackOffFactor(),
      processEngineConfiguration.getMaxBackOffTime(),
      processEngineConfiguration.isApplyJitter()
    );
  }

  private void initMaxBackOffLevel() {
    if (baseBackOffTime > 0
      && maxBackOffTime > 0
      && backOffFactor > 0
      && maxBackOffTime >= baseBackOffTime) {

      // calculate the maximum level that will produce the longest wait time < maxBackOffTime
      maxBackOffLevel = (int) log(backOffFactor, maxBackOffTime / baseBackOffTime) + 1;

      // + 1 to get the minimum level that produces a back off time > maxBackOffTime
      maxBackOffLevel += 1;
    }
    else {
      maxBackOffLevel = 0;
    }
  }

  protected double log(double base, double value) {
    return Math.log10(value) / Math.log10(base);
  }

  @Override
  public long getWaitTime() {

    if (backOffLevel == 0) {
      return 0;
    } else if (backOffLevel >= maxBackOffLevel) {
      return maxBackOffTime;
    } else {
      return calculateWaitTime();
    }
  }

  private long calculateWaitTime() {
    long waitTime = (long)(baseBackOffTime * Math.pow(backOffFactor, backOffLevel - 1));

    if (applyJitter) {
      waitTime += Math.random() + waitTime;
    }

    backOffFactor++;

    return waitTime;
  }
}
