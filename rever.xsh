$PROJECT = 'SHED'
$ACTIVITIES = ['version_bump',
               'changelog',
               'tag',
               'push_tag',
               'conda_forge',
               'ghrelease']

$VERSION_BUMP_PATTERNS = [
    ('shed/__init__.py', '__version__\s*=.*', "__version__ = '$VERSION'"),
    ('setup.py', 'version\s*=.*', "version='$VERSION',")
    ]
$CHANGELOG_FILENAME = 'CHANGELOG.rst'
$CHANGELOG_IGNORE = ['TEMPLATE']
$TAG_REMOTE = 'git@github.com:xpdAcq/SHED.git'

$GITHUB_REPO = 'SHED'
$GITHUB_ORG = 'xpdAcq'
