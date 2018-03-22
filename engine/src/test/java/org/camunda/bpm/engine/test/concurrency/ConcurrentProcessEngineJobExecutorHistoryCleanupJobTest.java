package org.camunda.bpm.engine.test.concurrency;

import org.camunda.bpm.engine.ProcessEngineConfiguration;
import org.camunda.bpm.engine.SchemaOperationsCommand;
import org.camunda.bpm.engine.impl.SchemaOperationsProcessEngineBuild;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.runtime.Job;

import java.util.List;

/**
 * <p>Tests a concurrent attempt of a bootstrapping Process Engine to reconfigure the HistoryCleanupJob while the JobExecutor tries to execute it.</p>
 *
 * @author Nikola Koevski
 */
public class ConcurrentProcessEngineJobExecutorHistoryCleanupJobTest extends ConcurrencyTestCase {

  @Override
  public void tearDown() throws Exception {
    ((ProcessEngineConfigurationImpl)processEngine.getProcessEngineConfiguration()).getCommandExecutorTxRequired().execute(new Command<Void>() {
      public Void execute(CommandContext commandContext) {

        List<Job> jobs = processEngine.getManagementService().createJobQuery().list();
        if (jobs.size() > 0) {
          assertEquals(1, jobs.size());
          String jobId = jobs.get(0).getId();
          commandContext.getJobManager().deleteJob((JobEntity) jobs.get(0));
          commandContext.getHistoricJobLogManager().deleteHistoricJobLogByJobId(jobId);
        }

        return null;
      }
    });
    super.tearDown();
  }

  public void testConcurrentHistoryCleanupJobReconfigurationExecution() throws InterruptedException {

    getProcessEngine().getHistoryService().cleanUpHistoryAsync(true);

    ThreadControl thread1 = executeControllableCommand(new ControllableJobExecutionCommand());
    thread1.reportInterrupts();
    thread1.waitForSync();

    ThreadControl thread2 = executeControllableCommand(new ControllableProcessEngineBootstrapCommand());
    thread2.reportInterrupts();
    thread2.waitForSync();

    thread1.makeContinue();
    thread1.waitForSync();

    thread2.makeContinue();

    Thread.sleep(2000);

    thread1.waitUntilDone();

    thread2.waitForSync();
    thread2.waitUntilDone();

    assertNull(thread1.getException());
    assertNotNull(thread2.getException());
  }

  protected static class ControllableProcessEngineBootstrapCommand extends ControllableCommand<Void> {

    @Override
    public Void execute(CommandContext commandContext) {

      SchemaOperationsCommand schemaOperationsCommand = new ControllableSchemaOperationsProcessEngineBuild(this.monitor);

      ProcessEngineConfiguration processEngineConfiguration = ProcessEngineConfiguration
        .createProcessEngineConfigurationFromResource("camunda.cfg.xml");
      processEngineConfiguration.setSchemaOperationsCommand(schemaOperationsCommand);

      processEngineConfiguration.buildProcessEngine();


      return null;
    }
  }

  protected static class ControllableJobExecutionCommand extends ControllableCommand<Void> {

    @Override
    public Void execute(CommandContext commandContext) {

      monitor.sync();

      Job historyCleanupJob = commandContext.getProcessEngineConfiguration().getHistoryService().findHistoryCleanupJob();

      commandContext.getProcessEngineConfiguration().getManagementService().executeJob(historyCleanupJob.getId());

      monitor.sync();

      return null;
    }
  }

  protected static class ControllableSchemaOperationsProcessEngineBuild extends SchemaOperationsProcessEngineBuild implements Command<Void> {

    protected final ThreadControl monitor;

    public ControllableSchemaOperationsProcessEngineBuild(ThreadControl threadControl) {
      this.monitor = threadControl;
    }

    @Override
    protected void createHistoryCleanupJob() {

      monitor.sync();

      super.createHistoryCleanupJob();

      monitor.sync();
    }
  }
}
