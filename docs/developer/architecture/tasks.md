# Asynchronous tasks in the service

The service aims to perform the minimum amount of work in the synchronous part of API calls, and offload work to asynchronous tasks.

There are two types of tasks: event-based and periodic.

## Event-based tasks

Tasks must be idempotent and not rely on ordering so that they can be delivered and retried without any race conditions.
Tasks must be independent to avoid deadlocks.

This flow chart depicts the tasks that each update to the store can trigger, and what store updates each task can trigger.

```mermaid
flowchart TD
    FltTemplateUpdated[(Fleet template updated)] --> FleetValidateTask[[FleetValidateTask]]
    FltSelectorUpdated[(Fleet selector updated)] --> FleetSelectorMatchTask[[FleetSelectorMatchTask]]
    FltConfigSourceUpdated[(Fleet config source updated)] --> FleetValidateTask[[FleetValidateTask]]
    RepoUpdated[(Repository updated)] --> RepositoryUpdatesTask[[RepositoryUpdatesTask]]
    ReposDeleted[(All repositories deleted)] --> RepositoryUpdatesTask[[RepositoryUpdatesTask]]
    FleetsDeleted[(All fleets deleted)] --> FleetSelectorMatchTask[[FleetSelectorMatchTask]]
    DevsDeleted[(All devices deleted)] --> FleetSelectorMatchTask[[FleetSelectorMatchTask]]
    DevLabelsUpdated[(Device labels updated)] --> FleetSelectorMatchTask[[FleetSelectorMatchTask]]
    DevSpecUpdated[(Device spec updated)] --> DeviceRenderTask[[DeviceRenderTask]]
    DevConfigSourceUpdated[(Device config source updated)] --> DeviceRenderTask[[DeviceRenderTask]]
    TemplateVersionCreated[(TemplateVersion created)] --> TemplateVersionPopulateTask[[TemplateVersionPopulateTask]]
    TemplateVersionValidated[(TemplateVersion validated)] --> FleetRolloutTask[[FleetRolloutTask]]
    DevLabelsUpdated --> FleetRolloutTask
    DevOwnerUpdated[(Device owner updated)] --> FleetRolloutTask

    FleetRolloutTask --> DevSpecUpdated
    FleetSelectorMatchTask --> DevOwnerUpdated
    FleetValidateTask --> TemplateVersionCreated
    TemplateVersionPopulateTask --> TemplateVersionValidated
    RepositoryUpdatesTask --> FltConfigSourceUpdated
    RepositoryUpdatesTask --> DevConfigSourceUpdated
```

## Periodic tasks

1. Try to access each repository and update its Status.
1. Check if each ResourceSync is up-to-date, and update resources if necessary.