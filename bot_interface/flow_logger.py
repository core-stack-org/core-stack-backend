from datetime import datetime
from bot_interface import models as bot_models
from bot_interface.tasks import process_work_demand_task


class FlowLoggerMixin:
    """
    Centralized flow completion logger for work_demand & grievance.
    """

    # ----------------- HELPERS -----------------

    def get_bot_instance(self, bot_instance_id):
        try:
            return bot_models.Bot.objects.get(id=bot_instance_id)
        except bot_models.Bot.DoesNotExist:
            print(f"Bot {bot_instance_id} not found, using fallback")
            return bot_models.Bot.objects.first()

    def get_user_session(self, user_id, bot):
        try:
            return bot_models.UserSessions.objects.get(user=user_id, bot=bot)
        except bot_models.UserSessions.DoesNotExist:
            print("UserSession with bot not found, trying without bot")
            return bot_models.UserSessions.objects.get(user=user_id)

    def get_bot_user(self, user_id):
        return bot_models.BotUsers.objects.get(id=user_id)

    def get_smj(self, smj_id):
        if not smj_id:
            return None
        try:
            return bot_models.SMJ.objects.get(id=smj_id)
        except bot_models.SMJ.DoesNotExist:
            print(f"SMJ {smj_id} not found")
            return None

    def build_community_context(self, user):
        context = {}
        active_community_id = (
            user.misc_data.get("active_community_id") if user.misc_data else None
        )
        if not active_community_id:
            return context

        try:
            from community_engagement.models import Community

            community = Community.objects.get(id=active_community_id)
            context = {
                "community_id": active_community_id,
                "community_name": (
                    community.project.name if community.project else "Unknown"
                ),
                "organization": (
                    community.project.organization.name
                    if community.project and community.project.organization
                    else "Unknown"
                ),
                "location_hierarchy": {},
            }

            for loc in community.locations.all():
                if loc.state:
                    context["location_hierarchy"]["state"] = loc.state.state_name
                if loc.district:
                    context["location_hierarchy"][
                        "district"
                    ] = loc.district.district_name
                if loc.block:
                    context["location_hierarchy"]["block"] = loc.block.block_name

        except Exception as e:
            print(f"Community context error: {e}")
            context = {
                "community_id": active_community_id,
                "error": "context_load_failed",
            }

        return context

    def create_user_log(self, *, app_type, bot, user, action, misc, smj=None):
        return bot_models.UserLogs.objects.create(
            app_type=app_type,
            bot=bot,
            user=user,
            key1="useraction",
            value1=action,
            key2="upload",
            value2="",  # used as processing flag
            key3="retries",
            value3="",
            key4="",
            misc=misc,
            smj=smj,
        )

    # ----------------- GENERIC LOGGER -----------------

    def log_flow_completion(
        self,
        *,
        bot_instance_id,
        data_dict,
        flow_key,
        misc_key,
        enable_async=False,
    ):
        try:
            user_id = int(data_dict.get("user_id"))

            bot = self.get_bot_instance(bot_instance_id)
            if not bot:
                return "failure"

            user_session = self.get_user_session(user_id, bot)
            bot_user = self.get_bot_user(user_id)
            smj = self.get_smj(data_dict.get("smj_id"))

            flow_data = (
                user_session.misc_data.get(misc_key, {})
                if user_session.misc_data
                else {}
            )

            if "photos" in flow_data:
                print(f"📷 Photos logged: {flow_data['photos']}")
                flow_data["photos_note"] = "HDPI WhatsApp media paths"

            misc = {
                f"{misc_key}_data": flow_data,
                "community_context": self.build_community_context(user_session),
                "flow_metadata": {
                    "smj_name": flow_key,
                    "completion_timestamp": datetime.now().isoformat(),
                    "user_number": (
                        bot_user.user.username if bot_user.user else "unknown"
                    ),
                    "session_id": f"session_{user_id}_{bot.id}",
                    "app_type": user_session.app_type,
                },
            }

            user_log = self.create_user_log(
                app_type=user_session.app_type,
                bot=bot,
                user=bot_user,
                action=flow_key,
                misc=misc,
                smj=smj,
            )

            print(f"✅ UserLogs created: {user_log.id}")

            if enable_async:
                process_work_demand_task.delay(user_log.id)
                print(f"🚀 Celery task queued for UserLogs {user_log.id}")

            return "success"

        except Exception as e:
            print(f"❌ log_flow_completion error [{flow_key}]: {e}")
            return "failure"

    # ----------------- PUBLIC METHODS -----------------

    def log_work_demand_completion(self, bot_instance_id, data_dict):
        return self.log_flow_completion(
            bot_instance_id=bot_instance_id,
            data_dict=data_dict,
            flow_key="work_demand",
            misc_key="work_demand",
            enable_async=True,
        )

    def log_grievance_completion(self, bot_instance_id, data_dict):
        return self.log_flow_completion(
            bot_instance_id=bot_instance_id,
            data_dict=data_dict,
            flow_key="grievance",
            misc_key="grievance",
            enable_async=False,
        )
