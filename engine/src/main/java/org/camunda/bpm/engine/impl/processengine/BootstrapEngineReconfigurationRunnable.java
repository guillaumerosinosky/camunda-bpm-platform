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

import org.camunda.bpm.engine.OptimisticLockingException;
import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.impl.ProcessEngineLogger;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.db.EnginePersistenceLogger;
import org.camunda.bpm.engine.impl.db.PersistenceSession;
import org.camunda.bpm.engine.impl.interceptor.CommandExecutor;

/**
 * @author Nikola Koevski
 */
public class BootstrapEngineReconfigurationRunnable implements Runnable {

  private final static EnginePersistenceLogger LOG = ProcessEngineLogger.PERSISTENCE_LOGGER;

  protected volatile boolean isInterrupted = false;

  protected ProcessEngine processEngine;
  protected CommandExecutor commandExecutor;
  protected ProcessEngineReconfigurationCommand bootstrapCommand;
  protected Exception optimisticLockingException;

  public BootstrapEngineReconfigurationRunnable(ProcessEngine processEngine) {
    this.processEngine = processEngine;
    this.commandExecutor = ((ProcessEngineConfigurationImpl)processEngine.getProcessEngineConfiguration())
      .getCommandExecutorSchemaOperations();
    this.bootstrapCommand = processEngine.getProcessEngineConfiguration().getProcessEngineReconfigurationCommand();
    this.optimisticLockingException = null;
  }

  @Override
  public void run() {

    ProcessEngineStrategy backOffStrategy = initializeBackOffStrategy();

    while (!isInterrupted) {
      try {
        commandExecutor.execute(bootstrapCommand);
        isInterrupted = true;
      } catch (OptimisticLockingException ex) {
        this.optimisticLockingException = ex;

        suspendReconfiguration(backOffStrategy.getWaitTime());
      }
    }
  }

  public void stop() {
    isInterrupted = true;
  }

  protected void suspendReconfiguration(long millis) {
    if (millis <= 0) {
      return;
    }

    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // ignore exception
    }
  }

  protected ProcessEngineStrategy initializeBackOffStrategy() {
    return new BackOffProcessEngineStrategy(processEngine.getProcessEngineConfiguration());
  }
}
