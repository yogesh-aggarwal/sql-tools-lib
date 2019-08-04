import click


class sqlite:
    def __init__(self):
        pass


class Inputs:
    @staticmethod
    @click.command()
    @click.option("--connect", default=None, prompt="Enter the database path")
    def databPath(connect):
        global databPath
        databPath = connect
        start()

    @click.command()
    @click.option("--commands", default="", prompt=">>> ")
    def command(self, commands):
        pass




@click.command()
def start():
    print(f"DatabPath: {databPath}")
    try:
        Inputs().command()
    except Exception as e:
        print(e)
        click.secho("\nThanks for using the shell.", fg="white", bold=True)



if __name__ == "__main__":
    click.secho("Welcome to SQL-Tools CLI tools.\nType help for more information.", fg="white", bold=True)
    Inputs.databPath()
    start()
