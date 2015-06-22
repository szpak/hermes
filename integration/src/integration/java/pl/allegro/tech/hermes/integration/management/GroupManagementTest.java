package pl.allegro.tech.hermes.integration.management;

import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;
import pl.allegro.tech.hermes.api.ErrorCode;
import pl.allegro.tech.hermes.api.Group;
import pl.allegro.tech.hermes.integration.IntegrationTest;

import javax.ws.rs.core.Response;

import static pl.allegro.tech.hermes.api.ErrorCode.VALIDATION_ERROR;
import static pl.allegro.tech.hermes.api.Group.Builder.group;
import static pl.allegro.tech.hermes.api.Topic.Builder.topic;
import static pl.allegro.tech.hermes.integration.test.HermesAssertions.assertThat;

public class GroupManagementTest extends IntegrationTest {

    @Test
    public void shouldCreateGroup() {
        // given when
        Response response = management.group().create(Group.from("groupManagementTestGroup"));

        // then
        assertThat(response).hasStatus(Response.Status.CREATED);
        Assertions.assertThat(management.group().list()).contains("groupManagementTestGroup");
    }

    @Test
    public void shouldUpdateGroup() {
        // given
        final String groupName = "groupManagementTestGroupToUpdate";
        final String supportTeam = "Skylab";
        operations.createGroup(groupName);
        Group modifiedGroup = group().withGroupName(groupName).withSupportTeam(supportTeam).build();

        // when
        management.group().update(groupName, modifiedGroup);

        // then
        Assertions.assertThat(management.group().get(groupName).getSupportTeam()).isEqualTo(supportTeam);
    }

    @Test
    public void shouldListGroups() {
        // given
        operations.createGroup("groupManagementTestListGroupsGroup1");
        operations.createGroup("groupManagementTestListGroupsGroup2");

        // when then
        Assertions.assertThat(management.group().list()).containsOnlyOnce("groupManagementTestListGroupsGroup1",
                                                                          "groupManagementTestListGroupsGroup2");
    }

    @Test
    void shouldCreateAndFetchGroupDetails() throws InterruptedException {
        //given
        Group group = new Group("groupManagementTestGroupWithDetails", "owner", "team", "contact");
        management.group().create(group);

        //when
        Group fetchedGroup = management.group().get(group.getGroupName());

        //then
        Assertions.assertThat(fetchedGroup).isEqualTo(group);
    }

    @Test
    public void shouldReturnBadRequestStatusWhenAttemptToCreateGroupWithInvalidCharactersWasMade() {
        // given
        Group groupWithNameWithSpaces = Group.from("group;` name with spaces");

        // when
        Response response = management.group().create(groupWithNameWithSpaces);

        // then
        assertThat(response).hasStatus(Response.Status.BAD_REQUEST).hasErrorCode(VALIDATION_ERROR);
    }

    @Test
    public void shouldRemoveGroup() {
        // given
        operations.createGroup("groupManagementTestRemoveGroup");

        // when
        Response response = management.group().delete("groupManagementTestRemoveGroup");

        // then
        assertThat(response).hasStatus(Response.Status.OK);
        assertThat(management.group().list()).doesNotContain("groupManagementTestRemoveGroup");
    }

    @Test
    public void shouldNotAllowOnRemovingNonEmptyGroup() {
        // given
        operations.createGroup("groupManagementTestRemoveNonemptyGroup");
        operations.createTopic(topic().withName("groupManagementTestRemoveNonemptyGroup", "topic").build());

        // when
        Response response = management.group().delete("groupManagementTestRemoveNonemptyGroup");

        // then
        assertThat(response).hasStatus(Response.Status.FORBIDDEN).hasErrorCode(ErrorCode.GROUP_NOT_EMPTY);
    }
}
