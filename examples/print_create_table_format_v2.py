
columns = ['organization_id',
'self',
'id',
'key',
'version',
'storyPoints',
'summary',
'statusStartTime',
'boards.names',
'createdAt',
'commentWithoutExternalMessageCount',
'votes',
'commentWithExternalMessageCount',
'deadline',
'updatedAt',
'favorite',
'updatedBy.display',
'type.display',
'priority.display',
'createdBy.display',
'assignee.display',
'queue.key',
'queue.display',
'status.display',
'previousStatus.display',
'parent.key',
'parent.display',
'components',
'sprint',
'epic.display',
'previousStatusLastAssignee.display',
'originalEstimation',
'spent',
'tags',
'estimation',
'checklistDone',
'checklistTotal',
'emailCreatedBy',
'sla',
'emailTo',
'emailFrom',
'lastCommentUpdatedAt',
'followers',
'pendingReplyFrom',
'end',
'start',
'project.display',
'votedBy.display',
'aliases',
'previousQueue.display',
'access',
'resolvedAt',
'resolvedBy.display',
'resolution.display',
'lastQueue.display']

for item in columns:
    print(item+' String,')