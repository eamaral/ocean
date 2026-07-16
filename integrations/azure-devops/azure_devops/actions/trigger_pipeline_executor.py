import httpx
from loguru import logger

from azure_devops.actions.abstract_ado_executor import AbstractAzureDevopsExecutor
from azure_devops.actions.exceptions import (
    InvalidActionParametersError,
    TriggerPipelineError,
)
from azure_devops.actions.utils import build_external_id
from azure_devops.client.azure_devops_client import RunPipelineOptions
from azure_devops.webhooks.webhook_processors.pipeline_run_action_webhook_processor import (
    PipelineRunActionWebhookProcessor,
)
from port_ocean.context.ocean import ocean
from port_ocean.core.models import IntegrationRun


class TriggerPipelineExecutor(AbstractAzureDevopsExecutor):
    """Executor for triggering Azure DevOps pipeline runs and tracking them.

    Azure DevOps' Run Pipeline API returns the created run synchronously, so the
    run is marked as started immediately (no polling). Completion is reported
    asynchronously by ``PipelineRunActionWebhookProcessor`` via the pipeline
    run-state-changed service hook.
    """

    ACTION_NAME = "trigger_pipeline"
    WEBHOOK_PROCESSOR_CLASS = PipelineRunActionWebhookProcessor
    WEBHOOK_PATH = "/webhook"

    async def execute(self, run: IntegrationRun) -> None:
        logger.info(f"Triggering pipeline for action run {run.id}", run_id=run.id)
        project_input = run.execution_properties.get("project")
        pipeline_id = run.execution_properties.get("pipelineId")
        if not (project_input and pipeline_id):
            raise InvalidActionParametersError("project and pipelineId are required")

        options = RunPipelineOptions(
            branch=run.execution_properties.get("branch"),
            template_parameters=run.execution_properties.get("templateParameters"),
            variables=run.execution_properties.get("variables"),
        )
        project = await self.client.get_single_project(str(project_input))
        if not project:
            raise InvalidActionParametersError(
                f"Project '{project_input}' was not found"
            )
        project_id = project["id"]

        try:
            pipeline_run = await self.client.run_pipeline(
                project_id, str(pipeline_id), options
            )
        except httpx.HTTPStatusError as e:
            raise TriggerPipelineError.from_response(
                e.response,
                f"Error triggering pipeline {pipeline_id} in project '{project_input}'",
            )

        external_id = build_external_id(
            project_id, str(pipeline_id), str(pipeline_run["id"])
        )
        link = pipeline_run.get("_links", {}).get("web", {}).get("href", "")
        await ocean.port_client.update_run_started(
            run,
            link,
            external_id,
            extra_output={"pipelineRunId": pipeline_run["id"]},
        )
