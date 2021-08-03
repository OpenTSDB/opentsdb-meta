CURRENT_VERSION=`git describe --abbrev=0 --tags 2>/dev/null` 
if [ $CURRENT_VERSION == '']
then
   CURRENT_VERSION="0.1.0"
fi   
echo "Current version $CURRENT_VERSION"
CURRENT_VERSION_PARTS=(${CURRENT_VERSION//./ })
VNUM1=${CURRENT_VERSION_PARTS[0]}
VNUM2=${CURRENT_VERSION_PARTS[1]}
VNUM3=${CURRENT_VERSION_PARTS[2]}
let val=$VNUM3+1
NEW_TAG="$VNUM1.$VNUM2.$val" 
echo "Committing new tag $NEW_TAG"
git tag $NEW_TAG
git push --tags
