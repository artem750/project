import * as React from 'react';
import ButtonSend from '../Button/ButtonSend.tsx';
import SelectOne from '../Select/SelectOne.tsx';
import axios from 'axios';
import Store from '../../store/Store.ts';

const currencys = ['rub', 'usd'];

const Autorization = () => {
    const [value, setValue] = React.useState<string>('');

    function getActives(val: string) {
        if (val !== '') {
            Store.setCurrentPage = 'freeOptimization';
            Store.setCurrency = val;
            axios
                .get(`http://127.0.0.1:8000/securities?currency=${val}`)
                .then((response) => {
                    Store.setCurrencys = response.data;
                })
                .catch((error) => {
                    console.error(error);
                });
        }
    }
    return (
        <div style={{display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '32px'}}>
            <h1 style={{ fontSize: '64px'}}>Выберите валюту</h1>
            <SelectOne data={currencys} title="Валюта" setCurrentValue={setValue} />
            <ButtonSend title={'Перейти к оптимизации'} onClick={() => getActives(value)} />
        </div>

    );
};

export default Autorization;
