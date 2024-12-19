import * as React from 'react';
import OutlinedInput from '@mui/material/OutlinedInput';
import InputLabel from '@mui/material/InputLabel';
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import ListItemText from '@mui/material/ListItemText';
import Select, { SelectChangeEvent } from '@mui/material/Select';
import Checkbox from '@mui/material/Checkbox';

const ITEM_HEIGHT = 48;
const ITEM_PADDING_TOP = 8;
const MenuProps = {
    PaperProps: {
        style: {
            maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
            width: 250,
        },
    },
};

interface MultipleSelectProps {
    data: string[];
    title: string;
    setCurrentValue: React.Dispatch<React.SetStateAction<string[]>>;
}

const MultipleSelect: React.FC<MultipleSelectProps> = ({ data, title, setCurrentValue }) => {
    const [personName, setPersonName] = React.useState<string[]>([]);

    const handleChange = (event: SelectChangeEvent<typeof personName>) => {
        const {
            target: { value },
        } = event;
        setPersonName(
            typeof value === 'string' ? value.split(',') : value,
        );
        setCurrentValue(typeof value === 'string' ? value.split(',') : value);
    };

    return (
        <div>
            <FormControl sx={{ m: 1, width: 400 }}>
                <InputLabel id="demo-multiple-checkbox-label">{title}</InputLabel>
                <Select
                    labelId="demo-multiple-checkbox-label"
                    id="demo-multiple-checkbox"
                    multiple
                    value={personName}
                    onChange={handleChange}
                    input={<OutlinedInput label={title} />}
                    renderValue={(selected) => selected.join(', ')}
                    MenuProps={MenuProps}
                >
                    {data.map((name, index) => (
                        <MenuItem key={index} value={name}>
                            <Checkbox checked={personName.includes(name)} />
                            <ListItemText primary={name} />
                        </MenuItem>
                    ))}
                </Select>
            </FormControl>
        </div>
    );
}
export default MultipleSelect
