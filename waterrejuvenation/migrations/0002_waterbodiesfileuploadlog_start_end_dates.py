from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("waterrejuvenation", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="waterbodiesfileuploadlog",
            name="start_date",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="waterbodiesfileuploadlog",
            name="end_date",
            field=models.DateField(blank=True, null=True),
        ),
    ]
