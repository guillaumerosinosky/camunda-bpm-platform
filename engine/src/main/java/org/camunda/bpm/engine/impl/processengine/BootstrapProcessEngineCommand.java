package org.camunda.bpm.engine.impl.processengine;

import org.camunda.bpm.engine.impl.ProcessEngineLogger;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.EnginePersistenceLogger;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.PropertyEntity;

/**
 *
 * @author Nikola Koevski
 */
public class BootstrapProcessEngineCommand implements ProcessEngineReconfigurationCommand {

  private final static EnginePersistenceLogger LOG = ProcessEngineLogger.PERSISTENCE_LOGGER;

  @Override
  public Void execute(CommandContext commandContext) {

    checkDeploymentLockExists(commandContext);
    checkHistoryCleanupLockExists(commandContext);
    createHistoryCleanupJob();

    return null;
  }

  protected void createHistoryCleanupJob() {
    if (Context.getProcessEngineConfiguration().getManagementService().getTableMetaData("ACT_RU_JOB") != null) {
      Context.getProcessEngineConfiguration().getHistoryService().cleanUpHistoryAsync();
    }
  }

  public void checkDeploymentLockExists(CommandContext commandContext) {
    PropertyEntity deploymentLockProperty = commandContext.getPropertyManager().findPropertyById("deployment.lock");
    if (deploymentLockProperty == null) {
      LOG.noDeploymentLockPropertyFound();
    }
  }

  public void checkHistoryCleanupLockExists(CommandContext commandContext) {
    PropertyEntity historyCleanupLockProperty = commandContext.getPropertyManager().findPropertyById("history.cleanup.job.lock");
    if (historyCleanupLockProperty == null) {
      LOG.noHistoryCleanupLockPropertyFound();
    }
  }
}
